Right now, the main file to run is [`crawler32.py`](/crawler32.py)

I originally used [`sql.py`](/sql.py) and then [`sql2.py`](/sql2.py) for the database interfacing, but after trying to add stemming, I realized I needed to rewrite that, so I made [`record.py`](/record.py).  There's also [`table.py`](/table.py) which helps with the pretty-printing.  The `lodify()` method also lives there.  `lod` stands for "List Of Dicts" and `lodify` converts a cursor into a LOD.  

Most of the other important files are `import`ed into `crawler32.py`.