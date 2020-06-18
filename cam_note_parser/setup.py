from setuptools import setup
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='cam_note_parser',
    version='0.0.1',
    author="John Falope",
    author_email='jfalope@outlook.com',
    description="Module for parsing CAM notes, matching attendee name and loading data to SnowFlake",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    # packages=[
    # ],

    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',

    url='git@github.com:olufal/olufaldb-sdk-package.git',
    install_requires=[
        'pandas==1.0.4',
        'snowflake-sqlalchemy==1.2.3',
        'SQLAlchemy==1.3.17',
        'probablepeople==0.5.4',
        'azure-storage-blob==12.3.2',
        'nltk==3.5'
    ],
    zip_safe=False,
)