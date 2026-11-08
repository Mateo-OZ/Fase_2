import io
import time

import pytest

from dfsha.common.crypto import FrameWriter, iter_frames, new_wrapped_dek, unwrap_dek
from dfsha.common.paths import PathError, authorize
from dfsha.common.tokens import TokenError, sign_block_token, verify_block_token


def test_token_valido_y_rechazos():
    t = sign_block_token({"b": "b1", "op": "r", "exp": time.time() + 60})
    assert verify_block_token(t, "b1", ("r",))["b"] == "b1"
    with pytest.raises(TokenError):
        verify_block_token(t, "b2", ("r",))
    with pytest.raises(TokenError):
        verify_block_token(t, "b1", ("w",))
    with pytest.raises(TokenError):
        verify_block_token(sign_block_token({"b": "b1", "op": "r", "exp": time.time() - 1}), "b1", ("r",))
    body, sig = t.split(".")
    with pytest.raises(TokenError):
        verify_block_token(body[:-2] + "AA." + sig, "b1", ("r",))


def test_cifrado_por_trozos():
    wrapped = new_wrapped_dek("f1")
    dek = unwrap_dek(wrapped, "f1")
    buf = io.BytesIO()
    w = FrameWriter(buf, "b1", dek)
    w.write(b"hola " * 1000)
    w.write(b"mundo")
    raw = buf.getvalue()
    assert b"hola" not in raw
    buf.seek(0)
    assert b"".join(iter_frames(buf, "b1", dek)) == b"hola " * 1000 + b"mundo"
    buf.seek(0)
    with pytest.raises(Exception):
        list(iter_frames(buf, "otro-bloque", dek))  # dato asociado distinto


def test_autorizacion_de_rutas():
    assert authorize("ana", "/home/ana/x/../y") == "/home/ana/y"
    for bad in ("/home/bob/x", "/home/ana/../bob", "/etc/passwd", "/home/anabel"):
        with pytest.raises(PathError):
            authorize("ana", bad)
