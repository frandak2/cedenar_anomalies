from invoke import task


@task
def lab(c, ip="*", port=8888):
    """Launch Jupyter lab"""
    print(f"Launching Jupyter lab at {ip}:{port}")
    c.run(
        f"jupyter lab --ip={ip} --port={port} --no-browser",
        pty=True,
    )


@task
def notebook(c, ip="*", port=8888):
    """Launch Jupyter notebook"""
    print(f"Launching Jupyter notebook at {ip}:{port}")
    c.run(
        f"jupyter notebook --ip={ip} --port={port} --no-browser",
        pty=True,
    )
