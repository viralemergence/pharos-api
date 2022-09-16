# testing/assertsql.py
# Copyright (C) 2005-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

import collections
import contextlib
import re

from .. import event
from .. import util
from ..engine import url
from ..engine.default import DefaultDialect
from ..engine.util import _distill_cursor_params
from ..schema import _DDLCompiles


class AssertRule(object):

    is_consumed = False
    errormessage = None
    consume_statement = True

    def process_statement(self, execute_observed):
        pass

    def no_more_statements(self):
        assert False, (
            "All statements are complete, but pending "
            "assertion rules remain"
        )


class SQLMatchRule(AssertRule):
    pass


class CursorSQL(SQLMatchRule):
    def __init__(self, statement, params=None, consume_statement=True):
        self.statement = statement
        self.params = params
        self.consume_statement = consume_statement

    def process_statement(self, execute_observed):
        stmt = execute_observed.statements[0]
        if self.statement != stmt.statement or (
            self.params is not None and self.params != stmt.parameters
        ):
            self.errormessage = (
                "Testing for exact SQL %s parameters %s received %s %s"
                % (
                    self.statement,
                    self.params,
                    stmt.statement,
                    stmt.parameters,
                )
            )
        else:
            execute_observed.statements.pop(0)
            self.is_consumed = True
            if not execute_observed.statements:
                self.consume_statement = True


class CompiledSQL(SQLMatchRule):
    def __init__(self, statement, params=None, dialect="default"):
        self.statement = statement
        self.params = params
        self.dialect = dialect

    def _compare_sql(self, execute_observed, received_statement):
        stmt = re.sub(r"[\n\t]", "", self.statement)
        return received_statement == stmt

    def _compile_dialect(self, execute_observed):
        if self.dialect == "default":
            dialect = DefaultDialect()
            # this is currently what tests are expecting
            # dialect.supports_default_values = True
            dialect.supports_default_metavalue = True
            return dialect
        else:
            # ugh
            if self.dialect == "postgresql":
                params = {"implicit_returning": True}
            else:
                params = {}
            return url.URL.create(self.dialect).get_dialect()(**params)

    def _received_statement(self, execute_observed):
        """reconstruct the statement and params in terms
        of a target dialect, which for CompiledSQL is just DefaultDialect."""

        context = execute_observed.context
        compare_dialect = self._compile_dialect(execute_observed)

        # received_statement runs a full compile().  we should not need to
        # consider extracted_parameters; if we do this indicates some state
        # is being sent from a previous cached query, which some misbehaviors
        # in the ORM can cause, see #6881
        cache_key = None  # execute_observed.context.compiled.cache_key
        extracted_parameters = (
            None  # execute_observed.context.extracted_parameters
        )

        if "schema_translate_map" in context.execution_options:
            map_ = context.execution_options["schema_translate_map"]
        else:
            map_ = None

        if isinstance(execute_observed.clauseelement, _DDLCompiles):

            compiled = execute_observed.clauseelement.compile(
                dialect=compare_dialect,
                schema_translate_map=map_,
            )
        else:
            compiled = execute_observed.clauseelement.compile(
                cache_key=cache_key,
                dialect=compare_dialect,
                column_keys=context.compiled.column_keys,
                for_executemany=context.compiled.for_executemany,
                schema_translate_map=map_,
            )
        _received_statement = re.sub(r"[\n\t]", "", util.text_type(compiled))
        parameters = execute_observed.parameters

        if not parameters:
            _received_parameters = [
                compiled.construct_params(
                    extracted_parameters=extracted_parameters
                )
            ]
        else:
            _received_parameters = [
                compiled.construct_params(
                    m, extracted_parameters=extracted_parameters
                )
                for m in parameters
            ]

        return _received_statement, _received_parameters

    def process_statement(self, execute_observed):
        context = execute_observed.context

        _received_statement, _received_parameters = self._received_statement(
            execute_observed
        )
        params = self._all_params(context)

        equivalent = self._compare_sql(execute_observed, _received_statement)

        if equivalent:
            if params is not None:
                all_params = list(params)
                all_received = list(_received_parameters)
                while all_params and all_received:
                    param = dict(all_params.pop(0))

                    for idx, received in enumerate(list(all_received)):
                        # do a positive compare only
                        for param_key in param:
                            # a key in param did not match current
                            # 'received'
                            if (
                                param_key not in received
                                or received[param_key] != param[param_key]
                            ):
                                break
                        else:
                            # all keys in param matched 'received';
                            # onto next param
                            del all_received[idx]
                            break
                    else:
                        # param did not match any entry
                        # in all_received
                        equivalent = False
                        break
                if all_params or all_received:
                    equivalent = False

        if equivalent:
            self.is_consumed = True
            self.errormessage = None
        else:
            self.errormessage = self._failure_message(params) % {
                "received_statement": _received_statement,
                "received_parameters": _received_parameters,
            }

    def _all_params(self, context):
        if self.params:
            if callable(self.params):
                params = self.params(context)
            else:
                params = self.params
            if not isinstance(params, list):
                params = [params]
            return params
        else:
            return None

    def _failure_message(self, expected_params):
        return (
            "Testing for compiled statement\n%r partial params %s, "
            "received\n%%(received_statement)r with params "
            "%%(received_parameters)r"
            % (
                self.statement.replace("%", "%%"),
                repr(expected_params).replace("%", "%%"),
            )
        )


class RegexSQL(CompiledSQL):
    def __init__(self, regex, params=None, dialect="default"):
        SQLMatchRule.__init__(self)
        self.regex = re.compile(regex)
        self.orig_regex = regex
        self.params = params
        self.dialect = dialect

    def _failure_message(self, expected_params):
        return (
            "Testing for compiled statement ~%r partial params %s, "
            "received %%(received_statement)r with params "
            "%%(received_parameters)r"
            % (
                self.orig_regex.replace("%", "%%"),
                repr(expected_params).replace("%", "%%"),
            )
        )

    def _compare_sql(self, execute_observed, received_statement):
        return bool(self.regex.match(received_statement))


class DialectSQL(CompiledSQL):
    def _compile_dialect(self, execute_observed):
        return execute_observed.context.dialect

    def _compare_no_space(self, real_stmt, received_stmt):
        stmt = re.sub(r"[\n\t]", "", real_stmt)
        return received_stmt == stmt

    def _received_statement(self, execute_observed):
        received_stmt, received_params = super(
            DialectSQL, self
        )._received_statement(execute_observed)

        # TODO: why do we need this part?
        for real_stmt in execute_observed.statements:
            if self._compare_no_space(real_stmt.statement, received_stmt):
                break
        else:
            raise AssertionError(
                "Can't locate compiled statement %r in list of "
                "statements actually invoked" % received_stmt
            )

        return received_stmt, execute_observed.context.compiled_parameters

    def _compare_sql(self, execute_observed, received_statement):
        stmt = re.sub(r"[\n\t]", "", self.statement)
        # convert our comparison statement to have the
        # paramstyle of the received
        paramstyle = execute_observed.context.dialect.paramstyle
        if paramstyle == "pyformat":
            stmt = re.sub(r":([\w_]+)", r"%(\1)s", stmt)
        else:
            # positional params
            repl = None
            if paramstyle == "qmark":
                repl = "?"
            elif paramstyle == "format":
                repl = r"%s"
            elif paramstyle == "numeric":
                repl = None
            stmt = re.sub(r":([\w_]+)", repl, stmt)

        return received_statement == stmt


class CountStatements(AssertRule):
    def __init__(self, count):
        self.count = count
        self._statement_count = 0

    def process_statement(self, execute_observed):
        self._statement_count += 1

    def no_more_statements(self):
        if self.count != self._statement_count:
            assert False, "desired statement count %d does not match %d" % (
                self.count,
                self._statement_count,
            )


class AllOf(AssertRule):
    def __init__(self, *rules):
        self.rules = set(rules)

    def process_statement(self, execute_observed):
        for rule in list(self.rules):
            rule.errormessage = None
            rule.process_statement(execute_observed)
            if rule.is_consumed:
                self.rules.discard(rule)
                if not self.rules:
                    self.is_consumed = True
                break
            elif not rule.errormessage:
                # rule is not done yet
                self.errormessage = None
                break
        else:
            self.errormessage = list(self.rules)[0].errormessage


class EachOf(AssertRule):
    def __init__(self, *rules):
        self.rules = list(rules)

    def process_statement(self, execute_observed):
        while self.rules:
            rule = self.rules[0]
            rule.process_statement(execute_observed)
            if rule.is_consumed:
                self.rules.pop(0)
            elif rule.errormessage:
                self.errormessage = rule.errormessage
            if rule.consume_statement:
                break

        if not self.rules:
            self.is_consumed = True

    def no_more_statements(self):
        if self.rules and not self.rules[0].is_consumed:
            self.rules[0].no_more_statements()
        elif self.rules:
            super(EachOf, self).no_more_statements()


class Conditional(EachOf):
    def __init__(self, condition, rules, else_rules):
        if condition:
            super(Conditional, self).__init__(*rules)
        else:
            super(Conditional, self).__init__(*else_rules)


class Or(AllOf):
    def process_statement(self, execute_observed):
        for rule in self.rules:
            rule.process_statement(execute_observed)
            if rule.is_consumed:
                self.is_consumed = True
                break
        else:
            self.errormessage = list(self.rules)[0].errormessage


class SQLExecuteObserved(object):
    def __init__(self, context, clauseelement, multiparams, params):
        self.context = context
        self.clauseelement = clauseelement
        self.parameters = _distill_cursor_params(
            context.connection, tuple(multiparams), params
        )
        self.statements = []

    def __repr__(self):
        return str(self.statements)


class SQLCursorExecuteObserved(
    collections.namedtuple(
        "SQLCursorExecuteObserved",
        ["statement", "parameters", "context", "executemany"],
    )
):
    pass


class SQLAsserter(object):
    def __init__(self):
        self.accumulated = []

    def _close(self):
        self._final = self.accumulated
        del self.accumulated

    def assert_(self, *rules):
        rule = EachOf(*rules)

        observed = list(self._final)
        while observed:
            statement = observed.pop(0)
            rule.process_statement(statement)
            if rule.is_consumed:
                break
            elif rule.errormessage:
                assert False, rule.errormessage
        if observed:
            assert False, "Additional SQL statements remain:\n%s" % observed
        elif not rule.is_consumed:
            rule.no_more_statements()


@contextlib.contextmanager
def assert_engine(engine):
    asserter = SQLAsserter()

    orig = []

    @event.listens_for(engine, "before_execute")
    def connection_execute(
        conn, clauseelement, multiparams, params, execution_options
    ):
        # grab the original statement + params before any cursor
        # execution
        orig[:] = clauseelement, multiparams, params

    @event.listens_for(engine, "after_cursor_execute")
    def cursor_execute(
        conn, cursor, statement, parameters, context, executemany
    ):
        if not context:
            return
        # then grab real cursor statements and associate them all
        # around a single context
        if (
            asserter.accumulated
            and asserter.accumulated[-1].context is context
        ):
            obs = asserter.accumulated[-1]
        else:
            obs = SQLExecuteObserved(context, orig[0], orig[1], orig[2])
            asserter.accumulated.append(obs)
        obs.statements.append(
            SQLCursorExecuteObserved(
                statement, parameters, context, executemany
            )
        )

    try:
        yield asserter
    finally:
        event.remove(engine, "after_cursor_execute", cursor_execute)
        event.remove(engine, "before_execute", connection_execute)
        asserter._close()
