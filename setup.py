from setuptools import setup
setup(
    name='extract_mbtiles',
    version='0.1',
    py_modules=['extract_mbtiles'],
    install_requires=[],
    entry_points='''
        [console_scripts]
        extract_mbtiles=extract_mbtiles:main
    ''',
)