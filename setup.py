from pathlib import Path

from setuptools import setup, find_packages

HERE = Path(__file__).parent

setup(
    name="datapipelines",
    version="0.1.0",
    description="Data transform pipeline library to simplify ETL-style applications",
    author="Deep Roots",
    author_email="",
    url="https://github.com/digdeeproots/DataPipeline.Python",
    python_requires=">=3.10.0",
    packages=find_packages(exclude=["tests*"]),
    package_data={"approvaltests": ["reporters/reporters.json"]},
    install_requires=["pyperclip==1.8.2", "pytest"],
    long_description=(HERE / "README.md").read_text(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS :: MacOS X",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries",
    ],
)
