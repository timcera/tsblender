# -*- coding: utf-8 -*-
"""Collection of functions for the manipulation of time series."""

from __future__ import absolute_import, division, print_function

import os.path
import sys
import warnings

import mando
from tstoolbox import tsutils

from .functions.get_series_wdm import get_series_wdm

warnings.filterwarnings("ignore")


@mando.command()
def about():
    """Display version number and system information."""
    tsutils.about(__name__)


function_library = {"get_series_wdm": get_series_wdm}


def get_blocks(seq):
    """Return blocks of lines between "START ..." lines.

    The block below from a tsproc file::

        START GET_SERIES_WDM
         CONTEXT context_1
         NEW_SERIES_NAME IN02329500 IN02322500
         FILE data_test.wdm
         DSN 1 2
         FILTER -999
        END GET_SERIES_WDM

    Should yield::

        [['start', 'get_series_wdm'],
         ['context', 'context_1'],
         ['new_series_name', 'IN02329500', 'IN02322500'],
         ['file', 'data_test.wdm'],
         ['dsn', '1', '2'],
         ['filter', '-999'],
         ['end', 'GET_SERIES_WDM']]

    The entire "START ..." line should be lower case, but all others should
    only have the first word lower cased.  This allows case sensitivity in the
    keyword value.
    """
    data = []
    for line in seq:
        nline = line.rstrip()

        # Handle comment lines and partial comment lines.  Everything after
        # a "#" is a comment.
        try:
            nline = nline[: nline.index("#")].rstrip()
        except ValueError:
            pass

        # Handle blank lines.
        if not nline:
            continue

        # Test for "START ..." at the beginning of the line, start collecting
        # lines and yield data when reaching the next "START ..."..
        words = nline.split()
        keyword = words[0].lower()
        if keyword == "start":
            words[1] = words[1].lower()
            if data:
                yield data
                data = []
        data.append([keyword] + words[1:])

    # Yield the last block.
    if data:
        yield data


@mando.command()
def run(infile, outfile=None, context=None):
    """Parse a tsproc file."""
    with open(infile, "r") as fpi:
        for i, group in enumerate(get_blocks(fpi), start=1):

            # Unroll the block.

            # First find the maximum number of words from each line in the
            # group and store in "maxl".
            maxl = 2
            for line in group:
                if line[0] in ["settings"]:
                    break
                if len(line) > maxl:
                    maxl = len(line)

            # Use "maxl" loops to create new groups.
            for unrolled in range(1, maxl):
                ngroup = []
                for line in group[:-1]:
                    # Take the "line[unrolled]" element if available,
                    # otherwise take the last element.
                    try:
                        ngroup.append([line[0]] + [line[unrolled]])
                    except IndexError:
                        ngroup.append([line[0]] + [line[-1]])
                print(ngroup)


def main():
    """Set debug and run mando.main function."""
    if not os.path.exists("debug_tsblender"):
        sys.tracebacklimit = 0
    mando.main()


if __name__ == "__main__":
    main()
