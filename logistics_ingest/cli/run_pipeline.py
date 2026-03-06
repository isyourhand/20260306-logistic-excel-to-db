from __future__ import annotations

from logistics_ingest.app import pipeline_service


def main() -> None:
    pipeline_service.run()


if __name__ == "__main__":
    main()
