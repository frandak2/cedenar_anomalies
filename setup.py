import os

from setuptools import find_packages, setup


def readme() -> str:
    """Utility function to read the README.md.

    Used for the `long_description`. It's nice, because now
    1) we have a top level README file and
    2) it's easier to type in the README file than to put a raw string in below.

    Args:
        nothing

    Returns:
        String of readed README.md file.
    """
    readme_path = os.path.join(os.path.dirname(__file__), "README.md")
    with open(readme_path, encoding="utf-8") as f:
        return f.read()


setup(
    name="cedenar_anomalies",
    version="0.1.0",
    author="Your name (or your organization/company/team)",
    author_email="Your email (or your organization/company/team)",
    description=(
        "Análisis y procesamiento de datos para detección de anomalías "
        "mediante clustering difuso."
    ),
    python_requires=">=3",
    license="",
    url="",
    packages=find_packages(),
    long_description="""
Análisis y procesamiento de datos relacionados con Zentry y CEDENAR, 
con el objetivo de identificar anomalías y realizar una clasificación 
mediante técnicas de clustering difuso. El proyecto integra diferentes 
fuentes de datos para crear un modelo predictivo y generar informes descriptivos.
""",
)
