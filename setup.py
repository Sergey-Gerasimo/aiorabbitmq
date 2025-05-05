from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="aiorabbitmq",
    version="0.1.0",
    author="Sergey",
    author_email="gerasimovsergey2004@gmail.com",
    description="Short description of your package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Sergey-Gerasimo/aiorabbitmq.git",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "aio-pika>=9.5.5",
        "aiormq>=6.8.1",
        "pydantic>=1.9.0",
        "typing-extensions>=4.3.0",
    ],
    extras_require={
        "dev": [
            "mypy>=0.910",
            "flake8>=4.0",
            "black>=22.0",
            "pytest>=7.0",
            "pytest-asyncio",
            "aio-pika",
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries",
        "Framework :: AsyncIO",
    ],
)
