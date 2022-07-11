def test(payload):
    try:
        if len(payload) == 6:
            return {"pass": True}
        else:
            return {"pass": False, "message": "NCBI ID must be six characters long"}

    except Exception as e:
        return {"pass": False, "message": e}
