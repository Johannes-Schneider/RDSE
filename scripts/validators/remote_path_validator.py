import argparse
from typing import Optional


class RemotePathValidator(argparse.Action):

    def __call__(self, parser: argparse.ArgumentParser, parser_namespace: object, values: str,
                 option_string: Optional[str] = None) -> None:
        if not values.startswith('~/'):
            values = '~/' + values
        setattr(parser_namespace, self.dest, values)
