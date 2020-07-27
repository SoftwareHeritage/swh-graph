# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import psutil
import sys
from pathlib import Path


def find_graph_jar():
    """find swh-graph.jar, containing the Java part of swh-graph

    look both in development directories and installed data (for in-production
    deployments who fecthed the JAR from pypi)

    """
    swh_graph_root = Path(__file__).parents[2]
    try_paths = [
        swh_graph_root / "java/target/",
        Path(sys.prefix) / "share/swh-graph/",
        Path(sys.prefix) / "local/share/swh-graph/",
    ]
    for path in try_paths:
        glob = list(path.glob("swh-graph-*.jar"))
        if glob:
            if len(glob) > 1:
                logging.warn(
                    "found multiple swh-graph JARs, " "arbitrarily picking one"
                )
            logging.info("using swh-graph JAR: {0}".format(glob[0]))
            return str(glob[0])
    raise RuntimeError("swh-graph JAR not found. Have you run `make java`?")


def check_config(conf):
    """check configuration and propagate defaults
    """
    conf = conf.copy()
    if "batch_size" not in conf:
        conf["batch_size"] = "1000000000"  # 1 billion
    if "max_ram" not in conf:
        conf["max_ram"] = str(psutil.virtual_memory().total)
    if "java_tool_options" not in conf:
        conf["java_tool_options"] = " ".join(
            [
                "-Xmx{max_ram}",
                "-XX:PretenureSizeThreshold=512M",
                "-XX:MaxNewSize=4G",
                "-XX:+UseLargePages",
                "-XX:+UseTransparentHugePages",
                "-XX:+UseNUMA",
                "-XX:+UseTLAB",
                "-XX:+ResizeTLAB",
            ]
        )
    conf["java_tool_options"] = conf["java_tool_options"].format(
        max_ram=conf["max_ram"]
    )
    if "java" not in conf:
        conf["java"] = "java"
    if "classpath" not in conf:
        conf["classpath"] = find_graph_jar()

    return conf


def check_config_compress(config, graph_name, in_dir, out_dir):
    """check compression-specific configuration and initialize its execution
    environment.
    """
    conf = check_config(config)

    conf["graph_name"] = graph_name
    conf["in_dir"] = str(in_dir)
    conf["out_dir"] = str(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    if "tmp_dir" not in conf:
        tmp_dir = out_dir / "tmp"
        conf["tmp_dir"] = str(tmp_dir)
    else:
        tmp_dir = Path(conf["tmp_dir"])
    tmp_dir.mkdir(parents=True, exist_ok=True)

    if "logback" not in conf:
        logback_confpath = tmp_dir / "logback.xml"
        with open(logback_confpath, "w") as conffile:
            conffile.write(
                """
<configuration>
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d %r %p [%t] %logger{1} - %m%n</pattern>
        </encoder>
    </appender>
    <root level="INFO">
        <appender-ref ref="STDOUT"/>
    </root>
</configuration>
"""
            )
        conf["logback"] = str(logback_confpath)

    conf["java_tool_options"] += " -Dlogback.configurationFile={logback}"
    conf["java_tool_options"] = conf["java_tool_options"].format(
        logback=conf["logback"]
    )

    return conf
