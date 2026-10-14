"""Compatibility wrapper; implementation lives in the single api/index.py file."""

import sys
from api.index import main as webhook_main


def main():
  arguments = sys.argv[1:]
  if '--info' in arguments:
    arguments = ['--webhook-info' if item == '--info' else item for item in arguments]
  else:
    arguments = ['--set-webhooks', *arguments]
  webhook_main(arguments)


if __name__ == '__main__':
  main()