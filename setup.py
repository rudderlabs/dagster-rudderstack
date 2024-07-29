from setuptools import find_packages, setup

setup(
    name="dagster_rudderstack",
    packages=find_packages(exclude=["dagster_rudderstack_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
