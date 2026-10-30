from pyspark.sql import SparkSession


def configure_session(spark: SparkSession) -> SparkSession:
    """Fixa o fuso da sessão em UTC.

    Os parquets da TLC guardam horário local de NY sem fuso. Com a sessão em UTC,
    o Spark lê e converte esses valores sem deslocar as horas — essencial para a
    análise por hora do dia (pergunta 2).
    """
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark