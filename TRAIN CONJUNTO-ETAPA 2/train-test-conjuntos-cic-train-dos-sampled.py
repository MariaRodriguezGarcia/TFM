import dask.dataframe as dd
import dask
import time
import pandas as pd
from dask.distributed import Client
from dask_ml.model_selection import train_test_split
from dask_ml.preprocessing import StandardScaler
from dask_ml.impute import SimpleImputer
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder

from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, ConfusionMatrixDisplay, roc_curve, auc, RocCurveDisplay
from sklearn.tree import DecisionTreeClassifier
from sklearn.svm import SVC
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB, BernoulliNB
from sklearn.linear_model import SGDClassifier
from sklearn.ensemble import BaggingClassifier, AdaBoostClassifier, RandomForestClassifier
from sklearn.neighbors import NearestCentroid
from sklearn.neural_network import MLPClassifier
from fpdf import FPDF
import matplotlib.pyplot as plt
import os
from sklearn import tree
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from sklearn.preprocessing import StandardScaler, LabelBinarizer
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import roc_curve, auc, accuracy_score
from sklearn.preprocessing import LabelBinarizer
from sklearn.metrics import (classification_report, accuracy_score, confusion_matrix, 
                             ConfusionMatrixDisplay, roc_curve, RocCurveDisplay, precision_score, recall_score, f1_score)
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
import numpy as np


# Create PDF with fpdf
class PDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 12)
        self.cell(0, 10, 'Classification Report with Metrics, Training, and Testing Time', 0, 1, 'C')

    def chapter_title(self, title):
        self.set_font('Arial', 'B', 12)
        self.cell(0, 10, title, 0, 1, 'L')
        self.ln(10)

    def chapter_body(self, body):
        self.set_font('Arial', '', 10)
        self.multi_cell(0, 5, body)
        self.ln()

    def add_image(self, image_path, title=''):
        if title:
            self.chapter_title(title)
        self.image(image_path, x=10, y=None, w=180)
        self.ln(10)
        
    def add_classification_report(self, report):
        self.chapter_title("Classification Report:")
        self.chapter_body(report)

# Define the chunk size
chunk_size = 10000  # You can adjust this based on your system's memory capacity

# Initialize an empty list to store the sampled chunks
sampled_chunks = []
csv_path = '/root/bbdd/logs-zeek/cic-iot-2023-encoded-common-12gb.csv'
# Iterate over the chunks in the CSV file
# Function to sample 30% of the DoS labeled rows
def sample_majority_class(df, label_col, majority_class, frac, random_state=None):
    majority_df = df[df[label_col] == majority_class]
    minority_df = df[df[label_col] != majority_class]
    
    sampled_majority_df = majority_df.sample(frac=frac, random_state=random_state)
    
    return pd.concat([sampled_majority_df, minority_df], ignore_index=True)

for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
    # Sample 50% of the chunk
    sampled_chunk = sample_majority_class(chunk, label_col='label', majority_class='DoS', frac=0.5, random_state=42)
    # Append the sampled chunk to the list
    sampled_chunks.append(sampled_chunk)

# Concatenate the sampled chunks into a single DataFrame
sampled_cic_df = pd.concat(sampled_chunks)

# Calculate the count of each label value
label_counts = sampled_cic_df['label'].value_counts()

# Print the label counts
print(label_counts)

# Define the chunk size
chunk_size = 10000  # You can adjust this based on your system's memory capacity

# Initialize an empty list to store the sampled chunks
sampled_chunks = []
csv_path = '/root/bbdd/logs-zeek/iot23-encoded-joint.csv'
# Iterate over the chunks in the CSV file
for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
    # Sample 50% of the chunk
    sampled_chunk = chunk.sample(frac=0.5, random_state=42)
    # Append the sampled chunk to the list
    sampled_chunks.append(sampled_chunk)

# Concatenate the sampled chunks into a single DataFrame
sampled_iot23_df = pd.concat(sampled_chunks)

# Calculate the count of each label value
label_counts = sampled_iot23_df['label'].value_counts()

# Print the label counts
print(label_counts)

csv_path = '/root/bbdd/logs-zeek/encoded_iotd20.csv'
iotd20_df = pd.read_csv(csv_path)

# Make sure all DataFrames have the same columns, irrespective of order
columns = list(sampled_iot23_df.columns)  # assuming iotd20_df has all the columns you need

# Reorder columns of each DataFrame to match the order in 'columns'
sampled_iot23_df = sampled_iot23_df[columns]
iotd20_df = iotd20_df[columns]
sampled_cic_df = sampled_cic_df[columns]

# List of DataFrames to concatenate
dataframes = [iotd20_df, sampled_iot23_df]
# Concatenate the DataFrames
concatenated_df = pd.concat(dataframes, ignore_index=True)

y_binary_test = concatenated_df['binary-label']

y_test = concatenated_df['label']
X_test = concatenated_df.drop(columns=['label','binary-label'])

y_train = sampled_cic_df['label']
X_train = sampled_cic_df.drop(columns=['label','binary-label'])
print("train and test sets ready")


del sampled_iot23_df, sampled_cic_df, concatenated_df
del dataframes



# Function to train and evaluate a single model
def train_and_evaluate_model(name, model, X_train, X_test, y_train, y_test):
    pdf = PDF()
    output_folder = f"/root/resultados-ml/conjunto/{name}-60test-cic-dos-sample-train"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    print(f"Start training {name}")
    start_time = time.time()
    if name in ["SGD", "MLP", "Nearest_Centroid"]:
        scaler = StandardScaler()
        X_train = scaler.fit_transform(X_train)
        X_test = scaler.transform(X_test)
        
        print(f"Scaling done for {name}")
        model.fit(X_train, y_train) #quitar si descomento svc
        """
        if name == "SVM":
            # Define parameter grid for grid search
            param_grid = {
                'C': [100, 1000],
                'gamma': [1e-4, 1e-5],
                'kernel': ['rbf', 'sigmoid']
            }
            # Perform grid search
            print(f"Start grid search for {name}")
            grid_search = GridSearchCV(SVC(probability=True), param_grid, refit=True, verbose=2, cv=5, n_jobs=-1)
            grid_search.fit(X_train, y_train)
            print(f"End grid search for {name}")
            # Use the best model with the best parameters
            model = grid_search.best_estimator_
            params = grid_search.best_params_
            single_start_time = time.time()
            model.fit(X_train, y_train)
            single_train_time = time.time() - single_start_time
            pdf.chapter_body(f"Best model: {model} Best params: {params} \n")
            pdf.chapter_body(f"Best model Training time: {single_train_time:.4f} seconds\n")
        else:
            model.fit(X_train, y_train)"""
    else:
        model.fit(X_train, y_train)
    train_time = time.time() - start_time
    print(f"End training {name}")
    start_time = time.time()
    print(f"Start prediction for {name}")
    y_pred = model.predict(X_test)
    test_time = time.time() - start_time

    pdf.add_page()
    pdf.chapter_title('Training and Testing Time')
    pdf.chapter_body(f"Training time: {train_time:.4f} seconds\nTesting time: {test_time:.4f} seconds\n")
    print(f"Creating reports for {name}")
    try:
        report = classification_report(y_test, y_pred)
        pdf.add_classification_report(report)
        print(f"Getting scores for {name}")
        precision_scores = precision_score(y_test, y_pred, average=None)
        recall_scores = recall_score(y_test, y_pred, average=None)

        pdf.chapter_body("Precision and Recall Scores by Class with 8 decimals:\n")
        unique_classes = np.unique(np.concatenate([y_test, y_pred]))
        for i, class_name in enumerate(unique_classes):
            precision = precision_scores[i]
            recall = recall_scores[i]
            pdf.chapter_body(f"Class '{class_name}':\n")
            pdf.chapter_body(f"  Precision: {precision:.8f}\n")
            pdf.chapter_body(f"  Recall: {recall:.8f}\n")

        # Define the classes of interest
        classes_of_interest = ["Scan", "benign", "DoS","BruteForce"]
        print(f"Getting confusion matrix for {name}")
        # Confusion matrix
        cm = confusion_matrix(y_test, y_pred, labels=classes_of_interest)
        disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=classes_of_interest)
        # Save the confusion matrix plot as an image file
        cm_plot_path = os.path.join(output_folder, "confusion_matrix.png")
        disp.plot()
        plt.savefig(cm_plot_path)

        # Add the confusion matrix plot to the PDF
        pdf.add_image(cm_plot_path, title="Confusion Matrix Plot")
        plt.show()
        plt.close()

        cm1 = confusion_matrix(y_test, y_pred, labels=classes_of_interest, normalize = 'true')
        disp1 = ConfusionMatrixDisplay(confusion_matrix=cm1, display_labels=classes_of_interest)
        # Save the confusion matrix plot as an image file
        cm1_plot_path = os.path.join(output_folder, "confusion_matrix_normalized.png")
        disp1.plot()
        plt.savefig(cm1_plot_path)

        # Add the confusion matrix plot to the PDF
        pdf.add_image(cm1_plot_path, title="Normalized Confusion Matrix Plot")
        plt.show()
        plt.close()

        if isinstance(model, DecisionTreeClassifier):
            print(f"Plotting tree for {name}")
            plt.figure(figsize=(25, 15))  # Adjust the size as needed
            # Plot the decision tree
            tree.plot_tree(model, feature_names=X_train.columns, filled=True, fontsize=8, proportion=True)

            # Save the decision tree plot as an image file
            tree_plot_path = os.path.join(output_folder, "decision_tree_default.png")
            plt.savefig(tree_plot_path)
            plt.close()
            # Add the decision tree plot to the PDF
            pdf.add_page()
            pdf.chapter_title('Decision Tree')
            pdf.add_image(tree_plot_path, title="Decision Tree Plot")

        print(f"Start ROC plotting for {name}")
        if name != "Nearest_Centroid":
            y_prob = model.predict_proba(X_test)
            
            label_binarizer = LabelBinarizer().fit(y_train)
            y_onehot_test = label_binarizer.transform(y_test)
            for class_of_interest in classes_of_interest:
                class_id = np.flatnonzero(label_binarizer.classes_ == class_of_interest)[0]
                fpr, tpr, _ = roc_curve(y_onehot_test[:, class_id], y_prob[:, class_id])
                display = RocCurveDisplay(fpr=fpr, tpr=tpr, estimator_name=f"{class_of_interest} vs the rest")
                display.plot(color="darkorange")
                plt.plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--')
                plt.xlabel('False Positive Rate')
                plt.ylabel('True Positive Rate')
                plt.title(f"One-vs-Rest ROC curve: {class_of_interest} vs (all other classes)")
                plt.legend(loc="lower right")
                plot_file = os.path.join(output_folder, f"roc_plot_{class_of_interest}.png")
                plt.savefig(plot_file)
                plt.show()
                plt.close()
                # Add the ROC curve plot to the PDF
                pdf.add_image(plot_file, title=f"ROC Curve: {class_of_interest} vs (all other classes)")
        else:
            print(f"ROC for Nearest Centroid for {name}")
            centroids = model.centroids_
            distances = np.linalg.norm(X_test[:, np.newaxis] - centroids, axis=2)
            label_binarizer = LabelBinarizer().fit(y_train)
            y_onehot_test = label_binarizer.transform(y_test)
            fpr = dict()
            tpr = dict()
            roc_auc = dict()
            for i, class_of_interest in enumerate(classes_of_interest):
                fpr[i], tpr[i], _ = roc_curve(y_onehot_test[:, i], -distances[:, i])
                roc_auc[i] = auc(fpr[i], tpr[i])
                plt.figure()
                display = RocCurveDisplay(fpr=fpr[i], tpr=tpr[i], estimator_name=f"{class_of_interest} vs the rest")
                display.plot(color="darkorange")
                plt.plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--')
                plt.xlabel('False Positive Rate')
                plt.ylabel('True Positive Rate')
                plt.title(f"One-vs-Rest ROC curve: {class_of_interest} vs (all other classes)")
                plt.legend(loc="lower right")
                plot_file = os.path.join(output_folder, f"roc_plot_{class_of_interest}.png")
                plt.savefig(plot_file)
                plt.show()
                plt.close()
                # Add the ROC curve plot to the PDF
                pdf.add_image(plot_file, title=f"ROC Curve: {class_of_interest} vs (all other classes)")
    except Exception as e:
            print(f"Error occurred for model {name}: {e}")
            pdf.add_page()
            pdf.chapter_title('Error')
            pdf.chapter_body(f"An error occurred during the training or evaluation of the model {name}:\n{str(e)}")
    
    finally:
        # Save PDF
        pdf_output_path = f"/root/resultados-ml/conjunto/conjunto-{name}-60test-cic-train-dos-sample-classification_report.pdf"
        pdf.output(pdf_output_path)
        print(f"PDF saved for {name}")
        return name, train_time, test_time, pdf_output_path

# Define models
models = {
    "Decision_Tree": DecisionTreeClassifier(),
    "Nearest_Centroid": NearestCentroid(),
    "Random_Forest": RandomForestClassifier(n_estimators=100, random_state=0),
    "Gaussian_NB": GaussianNB(),
    "Bernoulli_NB": BernoulliNB(),
    "SGD": SGDClassifier(loss='log_loss', max_iter=1000, tol=1e-3),
    "Bagging_Tree": BaggingClassifier(estimator=DecisionTreeClassifier(), n_estimators=100, random_state=0),
    "AdaBoost_Tree": AdaBoostClassifier(estimator=DecisionTreeClassifier(), n_estimators=100, random_state=0),
    "MLP": MLPClassifier(max_iter=1000, random_state=42),
    #"KNN": KNeighborsClassifier(),
    #"SVM": SVC(probability=True)
}

# Train models and generate reports in parallel
with ThreadPoolExecutor(max_workers=1) as executor:
    futures = {executor.submit(train_and_evaluate_model, name, model, X_train, X_test, y_train, y_test): name for name, model in models.items()}
    for future in as_completed(futures):
        name = futures[future]
        try:
            name, train_time, test_time, pdf_output_path = future.result()
            print(f"Completed {name}: Training time {train_time:.4f} seconds, Testing time {test_time:.4f} seconds, PDF saved at {pdf_output_path}")
        except Exception as exc:
            print(f"Error occurred for model {name}: {exc}")