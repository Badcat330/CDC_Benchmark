import setuptools
import subprocess
import sys

subprocess.check_call(["pip", "install", "-r", "requirements.txt"])

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='cdc_benchmark',
    version='0.1.0',
    description='This is console tool for testing cdc algorithems.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Alex Glushko',
    author_email='aglushko@hse.ru',
    url='https://github.com/Badcat330/CDC_Benchmark',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Development Status :: 1 - Planning"
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.9",
    install_requires=["psycopg2-binary", "mysql-connector-python", "pymssql", "json2table"],
    setup_requires=['pytest-runner'],
    tests_require=['pytest==4.4.1'],
    test_suite='tests',
    entry_points = {
        'console_scripts': ['cdc_benchmark=cdc_benchmark.__main__:main'],
    },
)
