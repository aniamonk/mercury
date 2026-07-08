"""Uvicorn entrypoint."""

from __future__ import annotations

import uvicorn

from mercury import config


def main() -> None:
    uvicorn.run("mercury.api.app:app", host=config.HOST, port=config.PORT)


if __name__ == "__main__":
    main()

