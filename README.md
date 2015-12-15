# VertNet Trait Parser

A project to extract specific traits from a slice of the VertNet database.

Extracting:
- sex
- life stage
- total body length
- total body mass

For sex and life stage we need to extract the value retrieved and the where we extracted the value from (CSV column and substring if gotten from an unformatted column cell). For the total body length and weight we need to also extract the units.

Currently, this is written in Perl. That may change later. To run:

    $ perl parse.pl <VertNet CSV file>

There are numerous issues with the current implementation that may get worked on if we want to change how or what is being parsed. I.e. This is not the most elegant code.

CPAN modules used:
- use Data::Dumper;
- use JSON;
- use Test::More;
- use Text::CSV_XS;
