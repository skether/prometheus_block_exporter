import hashlib
import shutil
from pathlib import Path


def get_sha256_digest(path):
    sha256 = hashlib.sha256()
    mv = memoryview(bytearray(128 * 1024))
    with open(path, 'rb', buffering=0) as f:
        while n := f.readinto(mv):
            sha256.update(mv[:n])
    return sha256.hexdigest()


class MismatchingHashError(Exception):
    """This is raised when a copied file's hashed don't match"""


class BlockCopier():
    def __init__(self, target_directory):
        self.target_directory = target_directory if isinstance(target_directory, Path) else Path(target_directory)
        self.hash_dictionary = {}

    def _copy2_with_hashing(self, src, dst, *, follow_symlinks=True, block_ulid):
        src_hash = get_sha256_digest(src)
        return_value = shutil.copy2(src=src, dst=dst, follow_symlinks=follow_symlinks)
        dst_hash = get_sha256_digest(dst)

        if src_hash != dst_hash:
            raise MismatchingHashError(f"Unable to copy {src}. Mismatching hashes!")

        relative_path = (Path(dst)).relative_to(self.target_directory)
        ((self.hash_dictionary[block_ulid])['files'])[str(relative_path)] = dst_hash
        
        return return_value

    def copy_block(self, block):
        if str(block.ulid) in self.hash_dictionary:
            raise ValueError(f"Block {str(block.ulid)} was already copied!")
        self.hash_dictionary[str(block.ulid)] = {'successful': False, 'files': {}}
        try:
            shutil.copytree(src=block.path,
                            dst=self.target_directory.joinpath(block.path.name),
                            copy_function=lambda src, dst, *, follow_symlinks=True: self._copy2_with_hashing(src=src, dst=dst, follow_symlinks=follow_symlinks, block_ulid=str(block.ulid)))
            self.hash_dictionary[str(block.ulid)]['successful'] = True
        except MismatchingHashError as e:
            if str(block.ulid) in self.hash_dictionary:
                del self.hash_dictionary[str(block.ulid)]
            shutil.rmtree(self.target_directory.joinpath(block.path.name), ignore_errors=True)
            print(e)
            raise e
