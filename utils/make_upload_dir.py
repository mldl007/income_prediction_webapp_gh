import shutil
import os


def make_upload_dir(path: str):
    if os.path.exists(path):
        shutil.rmtree(path=path, ignore_errors=True)
    os.makedirs(path)
