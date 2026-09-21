# SPDX-License-Identifier: Apache-2.0
"""The Fleetless Bridge: connects a ROS2 robot to the Fleetless cloud.

`__version__` is the single source of the version: setup.py reads it, the
bridge reports it to the cloud in the hello handshake, and a test keeps it in
step with package.xml.
"""

__version__ = "4.0.0"
