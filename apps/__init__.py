#
# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM.
# All rights reserved.
# This file is part of the CO3093/CO3094 course.
#
# StreapRous release
#
# The authors hereby grant to Licensee personal permission to use
# and modify the Licensed Source Code for the sole purpose of studying
# while attending the course
#

def create_sampleapp(ip, port):
    """Lazily import the sample app so service-only modules stay lightweight."""
    from .sampleapp import create_sampleapp as factory

    return factory(ip, port)


def create_tracker_app(ip, port):
    """Lazily import the tracker app so its pure services remain testable."""
    from .tracker_app import create_tracker_app as factory

    return factory(ip, port)
