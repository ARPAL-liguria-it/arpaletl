from setuptools import find_packages, setup

setup(
    name="arpaletl",
    packages=find_packages(),
    version="0.1.0-rc1",
    description="ARPAL ETL library",
    install_requires=[    
        'sqlalchemy',
        'oracledb',
        'pytest',
        'coverage',
        'pandas',
        'aiohttp'
    ],
)
