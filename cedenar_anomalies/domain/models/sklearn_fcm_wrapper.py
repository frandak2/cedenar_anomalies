# cedenar_anomalies/domain/models/sklearn_fcm_wrapper.py

import logging
from typing import Optional

from fcmeans import FCM
from sklearn.base import BaseEstimator, ClusterMixin


class SklearnFCMWrapper(BaseEstimator, ClusterMixin):
    def __init__(
        self,
        n_clusters=3,
        m=2.0,
        random_state=42,
        distance="euclidean",
        logger: Optional[logging.Logger] = None,
    ):
        self.n_clusters = n_clusters
        self.m = m
        self.random_state = random_state
        self.distance = distance
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    def fit(self, X, y=None):
        self.logger.info(f"Entrenando FCM con {self.n_clusters} clusters y m={self.m}")
        self.model_ = FCM(
            n_clusters=self.n_clusters,
            m=self.m,
            random_state=self.random_state,
            distance=self.distance,
        )
        self.model_.fit(X)
        self.labels_ = self.model_.predict(X)
        self.centers_ = self.model_.centers
        self.u_ = self.model_.u
        self.pc_ = self.model_.partition_coefficient
        self.pec_ = self.model_.partition_entropy_coefficient
        self.logger.info("Entrenamiento FCM completo.")
        return self

    def predict(self, X):
        self.logger.info("Realizando predicción con FCM.")
        return self.model_.predict(X)

    def soft_predict(self, X):
        self.logger.info("Realizando predicción con FCM.")
        return self.model_.soft_predict(X)


if __name__ == "__main__":
    main()
