import base64
import hashlib
import os
from Crypto import Random
from Crypto.Cipher import AES
from config import HASHED_KEY


def encrypt_str(mystr):
    byte_str = mystr.encode()
    raw = byte_str + (32 - len(byte_str) % 32) * chr(32 - len(byte_str) % 32).encode()
    iv = Random.new().read(AES.block_size)
    cipher = AES.new(HASHED_KEY, AES.MODE_CBC, iv)
    return base64.b64encode(iv + cipher.encrypt(raw)).decode()


def decrypt_str(mystr):
    enc = base64.b64decode(mystr)
    iv = enc[:AES.block_size]
    cipher = AES.new(HASHED_KEY, AES.MODE_CBC, iv)
    raw = cipher.decrypt(enc[AES.block_size:])
    return raw[:-ord(raw[len(raw)-1:])].decode()
