# Copyright (C) 2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from pathlib import Path
from typing import Dict

LOGBACK_CONF = b"""\
<configuration>
  <appender name="STDERR" class="ch.qos.logback.core.ConsoleAppender">
    <target>System.err</target>
    <encoder>
      <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} %msg%n</pattern>
    </encoder>
  </appender>

  <root level="debug">
    <appender-ref ref="STDERR" />
  </root>
</configuration>
"""
"""Overrides the default config, to log to stderr instead of stdout"""


def run_script(script: str, output_path: Path, **kwargs) -> None:
    """Passes ``kwargs`` to :func:`subprocess.run`."""
    import os
    import subprocess
    import tempfile

    from ..config import check_config

    conf: Dict = {}  # TODO: configurable

    output_path.parent.mkdir(parents=True, exist_ok=True)

    tmp_output_path = Path(f"{output_path}.tmp")

    conf = check_config(conf)

    with tempfile.NamedTemporaryFile(prefix="logback_", suffix=".xml") as logback_conf:
        logback_conf.write(LOGBACK_CONF)
        logback_conf.flush()

        java_tool_options = [
            f"-Dlogback.configurationFile={logback_conf.name}",
            conf["java_tool_options"],
        ]

        env = {
            **os.environ.copy(),
            "JAVA_TOOL_OPTIONS": " ".join(java_tool_options),
            "CLASSPATH": conf["classpath"],
        }

        with tmp_output_path.open("wb") as tmp_output:
            subprocess.run(
                ["bash", "-c", f"{script.strip()}"],
                stdout=tmp_output,
                env=env,
                check=True,
                **kwargs,
            )

    # Atomically write the output file
    tmp_output_path.replace(output_path)
