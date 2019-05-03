from pybuilder.core import use_plugin, init,Author

use_plugin("python.core")
use_plugin("python.unittest")
use_plugin("python.install_dependencies")
use_plugin("python.flake8")
use_plugin("python.coverage")
use_plugin("python.distutils")


name = "fraud_36"
default_task = "publish"


authors      = [Author("Olaya", "ayalo83@gmail.com")]
license      = "None"
default_task = "publish"

@init
def initialize(project):
    project.build_depends_on("mockito")

@init
def set_properties(project):
    pass
