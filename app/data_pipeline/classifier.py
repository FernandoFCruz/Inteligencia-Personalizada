import os
import joblib
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import LabelEncoder


# Caminho do modelo
MODEL_PATH = os.getenv(
    "TABLE_CLASSIFIER_PATH",
    "./app/agents/mapping_agent/table_classifier.joblib"
)


class TableClassifier:
    def __init__(self, model_path=MODEL_PATH):
        self.model_path = model_path
        self.model = None
        self.label_encoder = None

        # Embedder para classificação
        self.embedder = SentenceTransformer("all-MiniLM-L6-v2")

        # Carregar modelo se existir
        if os.path.exists(self.model_path):
            try:
                self._load()
            except Exception as e:
                print(f"⚠ Erro ao carregar classificador: {e}")
                self.model = None
                self.label_encoder = None

    def _load(self):
        data = joblib.load(self.model_path)
        self.model = data["model"]
        self.label_encoder = data["le"]

    def save(self, model, label_encoder):
        # Garantir que diretório exista
        os.makedirs(os.path.dirname(self.model_path), exist_ok=True)

        joblib.dump({"model": model, "le": label_encoder}, self.model_path)
        self.model = model
        self.label_encoder = label_encoder

    def train(self, texts, labels):
        """
        Train classifier.
        texts: list de perguntas
        labels: list de ids (schema.table)
        """

        # Gerar embeddings
        X = self.embedder.encode(texts, show_progress_bar=True)

        le = LabelEncoder()
        y = le.fit_transform(labels)

        clf = LogisticRegression(
            max_iter=1000,
            multi_class="multinomial",
            n_jobs=-1
        )
        clf.fit(X, y)

        # salvar modelo treinado
        self.save(clf, le)

        return clf, le

    def predict(self, question, top_k=5):
        """
        Retorna top_k tabelas mais prováveis baseado na pergunta.
        """

        if self.model is None or self.label_encoder is None:
            return []

        x = self.embedder.encode([question], show_progress_bar=False)

        # Distribuição de probabilidade
        probs = self.model.predict_proba(x)[0]

        # Índices ordenados
        idxs = np.argsort(probs)[::-1][:top_k]

        # Convertendo para rótulos reais
        labels = self.label_encoder.inverse_transform(idxs)
        scores = probs[idxs].tolist()

        return [
            {"id": lab, "score": float(s)}
            for lab, s in zip(labels, scores)
        ]