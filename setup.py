# setup.py
from setuptools import setup, find_packages

setup(
    name="diplom",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        'pandas>=1.5.0',
        'numpy>=1.23.0',
        'sqlalchemy>=2.0.0',
        'psycopg2-binary>=2.9.0',
        'python-dotenv>=1.0.0',
        'faker>=18.0.0',
    ],
    python_requires='>=3.9',
)