"""
A collection of miscellaneous tools.
"""
import configparser
import os


def get_config(file_name, header, path="core/config/"):
    """Get the configuration from a file in the luigi config path
    directory, and convert the key-value pairs under the 
    config :code:`header` into a `dict`.
    Parameters:
        file_name (str): The configuation file name.
        header (str): The header key in the config file.
    Returns:
        :obj:`dict`
    """
    conf_dir_path = find_filepath_from_pathstub(path)
    conf_path = os.path.join(conf_dir_path, file_name)
    config = configparser.ConfigParser()
    config.read(conf_path)
    return dict(config[header])


def get_paths_from_relative(relative=1):
    """A helper method for within :obj:`find_filepath_from_pathstub`.
    Prints all file and directory paths from a relative number of
    'backward steps' from the current working directory."""
    paths = []
    for root, subdirs, files in os.walk(f"./{'../'*relative}", followlinks=True):
        # Get all directory paths
        for subdir in subdirs:
            paths.append(os.path.join(root, subdir))
        # Get all file paths
        for f in files:
            paths.append(os.path.join(root, f))
    return map(os.path.abspath, paths)


def find_filepath_from_pathstub(path_stub):
    """Find the full path of the 'closest' file (or directory) to the current working
    directory ending with :obj:`path_stub`. The `closest` file is determined by
    starting forwards of the current working directory. The algorithm is then repeated
    by moving the current working directory backwards, one step at a time until
    the file (or directory) is found. If the HOME directory is reached, the algorithm
    raises :obj:`FileNotFoundError`.
    Args:
        path_stub (str): The partial file (or directory) path stub to find.
    Returns:
        The full path to the partial file (or directory) path stub.
    """
    relative = 0
    while True:
        for path in get_paths_from_relative(relative):
            if path.rstrip("/") == os.environ["HOME"]:
                raise FileNotFoundError(f"Could not find {path_stub}")
            if path.endswith(path_stub.rstrip("/")):
                return path
        relative += 1
