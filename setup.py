import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="elasticsearch_follow",
    version="0.1.4",
    author="Marc Schiereck",
    author_email="mdreem@fastmail.fm",
    description="An Elasticsearch tail",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mdreem/elasticsearch_follow",
    packages=setuptools.find_packages(exclude=['tests', 'examples']),
    install_requires=[
        'python-dateutil',
        'elasticsearch',
        'pytz'
    ],
    classifiers=[
        "Operating System :: OS Independent",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3"
    ],
    tests_require=['pytest',
                   'pytest-cov',
                   'pytest-html',
                   'elasticsearch>7.0.0,<=8.0.0'],
    test_suite="tests",
)
