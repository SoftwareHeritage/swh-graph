# Copyright (C) 2019-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from pathlib import Path
import sys

import psutil

logger = logging.getLogger(__name__)


def find_graph_jar():
    """find swh-graph.jar, containing the Java part of swh-graph

    look both in development directories and installed data (for in-production
    deployments who fecthed the JAR from pypi)

    """
    logger.debug("Looking for swh-graph JAR")
    swh_graph_root = Path(__file__).parents[2]
    try_paths = [
        swh_graph_root / "java/target/",
        Path(sys.prefix) / "share/swh-graph/",
        Path(sys.prefix) / "local/share/swh-graph/",
    ]
    for path in try_paths:
        logger.debug("Looking for swh-graph JAR in %s", path)
        glob = list(path.glob("swh-graph-*.jar"))
        if glob:
            if len(glob) > 1:
                logger.warning(
                    "found multiple swh-graph JARs, " "arbitrarily picking one"
                )
            logger.info("using swh-graph JAR: {0}".format(glob[0]))
            return str(glob[0])
    raise RuntimeError("swh-graph JAR not found. Have you run `make java`?")


def check_config(conf):
    """check configuration and propagate defaults"""
    conf = conf.copy()
    if "batch_size" not in conf:
        # Use 0.1% of the RAM as a batch size:
        # ~1 billion for big servers, ~10 million for small desktop machines
        conf["batch_size"] = min(int(psutil.virtual_memory().total / 1000), 2**30 - 1)
        logger.debug("batch_size not configured, defaulting to %s", conf["batch_size"])
    if "llp_gammas" not in conf:
        conf["llp_gammas"] = "-0,-1,-2,-3,-4"
        logger.debug("llp_gammas not configured, defaulting to %s", conf["llp_gammas"])
    if "max_ram" not in conf:
        conf["max_ram"] = str(int(psutil.virtual_memory().total * 0.9))
        logger.debug("max_ram not configured, defaulting to %s", conf["max_ram"])
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
        logger.debug(
            "java_tool_options not providing, defaulting to %s",
            conf["java_tool_options"],
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
    <appender name="STDERR" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d %r %p [%t] %logger{1} - %m%n</pattern>
        </encoder>
        <target>System.err</target>
    </appender>
    <root level="INFO">
        <appender-ref ref="STDERR"/>
    </root>
</configuration>
"""
            )
        conf["logback"] = str(logback_confpath)

    conf["java_tool_options"] += " -Dlogback.configurationFile={logback}"
    conf["java_tool_options"] += " -Djava.io.tmpdir={tmp_dir}"
    conf["java_tool_options"] = conf["java_tool_options"].format(
        logback=conf["logback"],
        tmp_dir=conf["tmp_dir"],
    )

    return conf
