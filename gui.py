import pandas as pd
from PyQt5.QtGui import QPixmap
from PyQt5.QtWidgets import QGroupBox, QFormLayout, QLabel, QComboBox, QLineEdit, QDialogButtonBox, \
    QVBoxLayout, QDialog, QWidget, QPushButton, QTextEdit, QMainWindow
from matplotlib import pyplot as plt

from config import *
from utils import *
import seaborn as sns


class TestResultsDialog(QDialog):
    def __init__(self, db_type, plot):
        super().__init__()
        self.db_type = db_type
        self.setWindowTitle("Test scenarios results - " + db_type)
        self.layout = QVBoxLayout(self)

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
    def __init__(self, db_type, dataset):
        super().__init__()
        self.dataset = dataset
        self.db_type = db_type
        self.setWindowTitle("Testing - " + db_type)
        self.setGeometry(200, 200, 600, 150)
        self.layout = QVBoxLayout(self)
        self.table_size_label = QLabel("Table sizes:")
        self.table_size = QLineEdit("100, 1000, 10000, 100000")
        self.layout.addWidget(self.table_size_label)
        self.layout.addWidget(self.table_size)
        self.run_button = QPushButton("Run tests")
        self.run_button.clicked.connect(self.run_tests)
        self.layout.addWidget(self.run_button)

    def run_tests(self):
        print("Testing scenarios for:" + self.db_type)
        table_size = [int(size) for size in self.table_size.text().split(',')]
        results = []
        for size in table_size:
            print("Table size: " + str(size))
            results_df = run_basic_tests(databases[self.db_type](size, self.dataset), db_params[self.dataset])
            results_df['database'] = self.db_type
            results_df['table_size'] = size
            results_df['dataset'] = self.dataset
            results.append(results_df)

        out_df = pd.concat(results)
        out_df.to_csv("results.csv", mode='w')
        out_df = out_df.melt(id_vars=["database", "table_size", "dataset"])

        plt.subplots(figsize=(12, 5))
        sns.barplot(y=out_df.variable, x=out_df.value.dt.microseconds, hue=out_df.table_size, orient="h")
        plt.xlabel("Time [ms]")
        plt.ylabel("Test name")
        plt.xscale("log")
        plt.savefig("./out.png")
        img_path = "./out.png"

        results_dialog = TestResultsDialog(self.db_type, img_path)
        results_dialog.exec_()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setGeometry(300, 300, 600, 200)
        self.setWindowTitle("ZTBD PROJ1")

        self.central_widget = QWidget(self)
        self.setCentralWidget(self.central_widget)

        self.layout = QVBoxLayout(self.central_widget)
        self.database_combo = QComboBox()
        self.database_combo_label = QLabel("Choose Database for testing:")
        self.layout.addWidget(self.database_combo_label)

        self.database_combo.addItem("PostgreSQL")
        self.database_combo.addItem("MongoDB")
        self.database_combo.addItem("Redis")
        self.layout.addWidget(self.database_combo)

        self.dataset_combo_label = QLabel("Choose Dataset:")
        self.layout.addWidget(self.dataset_combo_label)
        self.dataset_combo = QComboBox()
        self.dataset_combo.addItem("Yelp")
        self.dataset_combo.addItem("IMDB")
        self.layout.addWidget(self.dataset_combo)

        self.query_button = QPushButton("Choose")
        self.query_button.clicked.connect(self.open_query_form)

        self.layout.addWidget(self.query_button)
        self.query_form = None

    def open_query_form(self):
        db_type = self.database_combo.currentText()
        self.query_form = QueryFormWindow(db_type, self.dataset_combo.currentText())
        self.query_form.show()
