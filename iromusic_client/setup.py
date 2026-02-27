"""
Setup configuration for iromusic_client package.
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read README
readme_file = Path(__file__).parent / "README.md"
long_description = readme_file.read_text(encoding="utf-8") if readme_file.exists() else ""

setup(
    name="iromusic_client",
    version="1.0.0",
    description="Python client for iromusicapp.ir API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="iromusic_client",
    author_email="dissonancee@gmail.com",
    url="https://github.com/qcomputer/iromusic_client",
    package_dir={"": "src"},
    packages=["iromusic_client"],
    python_requires=">=3.8",
    install_requires=[
        "requests>=2.28.0",
        "urllib3>=1.26.0",
        "click>=8.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "pytest-mock>=3.10.0",
            "flake8>=5.0.0",
            "black>=22.0.0",
            "mypy>=0.990",
        ],
    },
    entry_points={
        "console_scripts": [
            "iromusic=iromusic_client.cli:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.14",
        "Topic :: Internet :: HTTP Clients",
    ],
    keywords="api client iromusic scraper",
    project_urls={
        "Bug Reports": "https://github.com/example/iromusic_client/issues",
        "Source": "https://github.com/example/iromusic_client",
    },
)
