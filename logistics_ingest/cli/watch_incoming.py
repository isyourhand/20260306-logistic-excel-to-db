from __future__ import annotations

from logistics_ingest.app import watcher_service


def main() -> None:
    watcher_service.run()


if __name__ == "__main__":
    main()
