import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="bonobo-trans",
    version="0.1.4",
    author="S.R.G. Venema",
    author_email="srg.venema@gmail.com",
    description="ETL transformations",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Sjoerd82/bonobo-trans",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
)