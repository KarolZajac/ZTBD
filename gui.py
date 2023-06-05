from PyQt5.QtGui import QPixmap
from PyQt5.QtWidgets import QGroupBox, QFormLayout, QLabel, QComboBox, QLineEdit, QDialogButtonBox, \
    QVBoxLayout, QDialog, QWidget, QPushButton, QTextEdit, QMainWindow
from config import *
from utils import *


class TestResultsDialog(QDialog):
    def __init__(self, db_type, results_df, plot):
        super().__init__()
        self.db_type = db_type
        self.setWindowTitle("Test scenarios results - " + db_type)
        self.layout = QVBoxLayout(self)

        # Create a QTextEdit to display the dataframe
        self.dataframe_text = QTextEdit(self)
        self.dataframe_text.setReadOnly(True)
        self.dataframe_text.setText(results_df.to_string())
        self.layout.addWidget(self.dataframe_text)

        # Create a QLabel to display the plot image
        self.plot_label = QLabel(self)
        pixmap = QPixmap(plot)
        self.plot_label.setPixmap(pixmap)
        self.layout.addWidget(self.plot_label)

        # Create a QPushButton to close the dialog
        self.close_button = QPushButton("Close", self)
        self.close_button.clicked.connect(self.close)
        self.layout.addWidget(self.close_button)
        print("Results Dialog Opened!")


class QueryFormWindow(QWidget):
    def __init__(self, db_type):
        super().__init__()
        self.db_type = db_type
        self.setWindowTitle("Testing - " + db_type)
        self.setGeometry(200, 200, 600, 400)
        self.layout = QVBoxLayout(self)
        self.table_size = QLineEdit("100")
        self.layout.addWidget(self.table_size)
        self.run_button = QPushButton("Run")
        self.run_button.clicked.connect(self.run_tests)
        self.layout.addWidget(self.run_button)

    def run_tests(self):
        print("Testing scenarios for:" + self.db_type)
        table_size = int(self.table_size.text())
        print("Table size: " + str(table_size))
        results_df = run_basic_tests(databases[self.db_type](table_size))
        results_df['database'] = self.db_type
        results_df['table_size'] = table_size
        # Open the ResultsDialog and pass the results
        img_path = "./out.png"
        results_dialog = TestResultsDialog(self.db_type, results_df, img_path)
        results_dialog.exec_()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setGeometry(300, 300, 600, 400)
        self.setWindowTitle("ZTBD PROJ1")

        self.central_widget = QWidget(self)
        self.setCentralWidget(self.central_widget)

        self.layout = QVBoxLayout(self.central_widget)
        self.database_combo = QComboBox()
        self.database_combo.addItem("PostgreSQL")
        self.database_combo.addItem("MongoDB")
        self.database_combo.addItem("Redis")

        self.layout.addWidget(self.database_combo)

        self.query_button = QPushButton("Open Query Form")
        self.query_button.clicked.connect(self.open_query_form)
        self.layout.addWidget(self.query_button)
        self.query_form = None

    def open_query_form(self):
        db_type = self.database_combo.currentText()
        self.query_form = QueryFormWindow(db_type)
        self.query_form.show()
