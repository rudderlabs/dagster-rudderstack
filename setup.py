from setuptools import find_packages, setup

setup(
    name="dagster-rudderstack",
    packages=find_packages(exclude=["rudder_dagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
