"""`python -m tributary.loaders.clickhouse --channel <name> --destination <name> --env <env>
--bootstrap <hosts>`."""

from .main import main

if __name__ == "__main__":
    main()
