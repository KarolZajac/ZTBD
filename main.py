from PyQt5.QtWidgets import QApplication

from gui import MainWindow
import sys
import os.path


def main():
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    plugin_path = os.path.join("./venv/Lib/site-packages/PyQt5/Qt5", "plugins", "platforms")
    os.environ['QT_QPA_PLATFORM_PLUGIN_PATH'] = plugin_path
    main()
