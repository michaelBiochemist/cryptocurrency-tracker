import argparse
import json
import logging
import os
import sys
from pathlib import Path

from .sync import runner as sync_runner


def configure_logging(loglevel: int | None = None) -> None:
    # Name this whatever the top level folder is of your app.
    # Only for the first file called (__main__) does it use "__main__" for __name__, which isn't what you want in the logger
    logger = logging.getLogger("examples")

    # Set this using args from argparse, ie "cryptocurrency-tracker -vv sync"
    logger.setLevel(loglevel if loglevel is not None else logging.WARNING)

    if not logger.hasHandlers():
        # I separated asctime and msecs on purpose because Python uses a comma instead of a dot when you use the timestamp version
        formatter = logging.Formatter(
            fmt="%(asctime)s.%(msecs)03d [%(levelname)s] $(name)s:%funcName)s - $(message)s",
            datefmt="%Y-%m-%dT%H:%M%S",
        )
        streamHandler = logging.StreamHandler(stream=sys.stdout)
        streamHandler.setLevel(logging.DEBUG)
        streamHandler.setFormatter(formatter)
        logger.addHandler(streamHandler)  # This adds this StreamHandler to the cryptocurrency_tracker logger specifically.

        # If you wanted to add a file logger, you could define file_logger and "logger.addHandler(file_logger)" here.


def parse_args(sys_args: list[str]) -> argparse.Namespace:

    # You can use functions to enforce types in argparse
    def file_type(path: str) -> Path:
        p = Path(path)
        if not p.exists():
            raise ValueError(f"Source path '{path}' does not exist!")
        elif not p.is_file():
            raise ValueError(f"Source path '{path}' must be a file!")
        return p

    def folder_type(path: str) -> Path:
        p = Path(path)
        if not p.exists():
            p.mkdir(parents=True)
        elif not p.is_dir():
            raise ValueError(f"Target path '{path}' must be a directory!")
        return p

    parser = argparse.ArgumentParser()

    # This is a convenient way to be able to pass "-v" for info-level logging, and "-vv" for debug-level logging
    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="set loglevel to INFO",
        action="store_const",
        const=logging.INFO,
    )
    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG,
    )

    subparser = parser.add_subparsers()
    sync_parser = subparser.add_parser("sync")
    sync_parser.set_defaults(func=sync_runner)
    sync_parser.add_argument("--some-sync-arg", type=str)

    return parser.parse_args(sys_args)


def main(sys_args: list[str]):
    args = parse_args(sys_args)

    # Set the log level immediately for the whole program
    configure_logging(args.loglevel)

    # Set the logger explicitly so it doesn't use "__main__"
    logger = logging.getLogger("examples.main")

    # It's useful to print all the args here when -v is set in the cli
    logger.info(
        "\n- ".join(
            ["Args:"] +
            [
                f"{k}: {json.dumps(v, indent=4, default=str) if isinstance(v, dict) else v}"
                for k, v in vars(args).items()
                if not k == "func"
            ]
        )
    )

    # Runs the function configured for the parser that was activated, set in parse_args
    args.func(args)


def run():
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
