import sqlite3
import math
import argparse

def lonlat_to_tile(zoom, lon_deg, lat_deg):
    lat_rad = math.radians(lat_deg)
    n = 2.0 ** zoom
    x = (lon_deg + 180.0) / 360.0 * n
    y = (1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n
    return x, n-y-1

def optimize_connection(cur):
    cur.execute("""PRAGMA synchronous=0""")
    cur.execute("""PRAGMA locking_mode=EXCLUSIVE""")
    cur.execute("""PRAGMA journal_mode=DELETE""")

def optimize_database(cur):
    cur.execute("""ANALYZE;""")
    cur.execute("""VACUUM;""")

def set_metadata(cur, name, value):
    cur.execute("INSERT OR REPLACE INTO metadata (name, value) values (?,?)", (name, value))

# Create the command line argument parser
parser = argparse.ArgumentParser(description='Extract tiles from one MBTiles file to another.')
parser.add_argument('--min-zoom', type=int, required=True, help='Minimum zoom level')
parser.add_argument('--max-zoom', type=int, required=True, help='Maximum zoom level')
parser.add_argument('--region', type=float, nargs=4, metavar=('MIN_LON', 'MIN_LAT', 'MAX_LON', 'MAX_LAT'), required=True)
parser.add_argument('--source', type=str, required=True, help='Source MBTiles file path')
parser.add_argument('--destination', type=str, required=True, help='Destination MBTiles file path')

# Parse the command line arguments
args = parser.parse_args()

# Connect to the source MBTiles SQLite database
source_conn = sqlite3.connect(args.source)
source_cur = source_conn.cursor()

# Create the destination MBTiles SQLite database
dest_conn = sqlite3.connect(args.destination)
dest_cur = dest_conn.cursor()

optimize_connection(dest_cur)

# Create the tiles table in the destination MBTiles file
dest_cur.execute('CREATE TABLE IF NOT EXISTS tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB)')

# Create the metadata table in the destination MBTiles file
dest_cur.execute('CREATE TABLE IF NOT EXISTS metadata (name TEXT, value TEXT)')

# Build an index on the tiles table for faster querying
dest_cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS tile_index ON tiles (zoom_level, tile_column, tile_row)')

dest_cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS metadata_index ON metadata (name)')

# Copy the metadata over
source_cur.execute('SELECT name, value from metadata')
for row in source_cur:
    dest_cur.execute("INSERT OR REPLACE INTO metadata VALUES(?, ?)", row)

min_zoom = None
max_zoom = None

# Iterate over zoom levels
for zoom in range(args.min_zoom, args.max_zoom + 1):
    num_tiles = 0
    min_x, min_y = lonlat_to_tile(zoom, args.region[0], args.region[1])
    max_x, max_y = lonlat_to_tile(zoom, args.region[2], args.region[3])

    # Expand the bounds so we get the tiles on the edges
    min_x = math.floor(min_x)
    min_y = math.floor(min_y)
    max_x = math.floor(max_x)
    max_y = math.floor(max_y)

    source_cur.execute('SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles WHERE zoom_level = ? AND tile_column BETWEEN ? AND ? AND tile_row BETWEEN ? AND ?', (zoom, min_x, max_x, min_y, max_y))

    for row in source_cur:
        num_tiles +=1
        dest_cur.execute('INSERT OR REPLACE INTO tiles VALUES (?, ?, ?, ?)', row)

    if num_tiles > 0 and min_zoom is None:
        min_zoom = zoom

    if num_tiles > 0 and (max_zoom is None or zoom > max_zoom):
        max_zoom = zoom

    print("Level %s copied %s tiles" % (zoom, num_tiles))

# Update the min and max zoom if necessary
if min_zoom is not None:
    set_metadata(dest_cur, 'minzoom', min_zoom)

if max_zoom is not None:
    set_metadata(dest_cur, 'maxzoom', max_zoom)

# Commit and close the destination MBTiles connection
dest_conn.commit()
optimize_database(dest_cur)
dest_conn.close()

print('Tiles have been extracted from the source and added to the destination MBTiles file.')
