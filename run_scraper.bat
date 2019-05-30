CD /D %~dp0

@echo off

CALL pcvenv\scripts\activate.bat

REM Arguments for main are:
REM - ScrapeComercios (True/False)
REM - Scrape list of EANs for comercio (True) or reuse old (False)
REM - ScrapePromos (True/False)

python -c "import scraper;scraper.main(True,False,True)"

PAUSE

deactivate

