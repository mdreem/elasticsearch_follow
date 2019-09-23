import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="elasticsearch_follow",
    version="0.0.1",
    author="Marc Schiereck",
    author_email="mdreem@fastmail.fm",
    description="An Elasticsearch tail",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mdreem/elasticsearch_follow",
    packages=setuptools.find_packages(),
    classifiers=[
        "Operating System :: OS Independent",
    ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest',
                   'pytest-cov',
                   'pytest-html',
                   'elasticsearch>=6.0.0,<7.0.0'],
    test_suite="tests",
)
