# SteamTags

## Scripts description:
1) **tags_parser.py** — Script for multi-threads popular tags parsing for all games from *app_id_info* MySQL table. It creates pickle file into script's directory into "[timestamp]_tags.pickle" file;
2) **pickle2sql.py** — Script for turning pickle file created by previous script into SQL table;
3) **getting_small_piece2csv.py** — Script for collecting info from *games_2* SQL table for N first *steamids* to csv file;
4) **create_gs_tables.py** — Script for collecting all games info into certain directory.

## Notebook description:
In **Games data analysis.ipynb** I described different methods for games tags correlation analysis.
