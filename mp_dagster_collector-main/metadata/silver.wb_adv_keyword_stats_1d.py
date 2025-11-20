import dagster as dg

def metadata():
    return {
        "link_to_docs": dg.MetadataValue.url("https://wiki.yandex.ru/homepage/product/potoki-dannyx/wb-adv-stats-keywords/"),
        "snippet": dg.MetadataValue.md("# Статистика РК по ключевым фразам.\n Для автоматических кампаний и аукционов"),
        "dagster/column_schema": dg.TableSchema(
            columns=[
                dg.TableColumn(
                    name="company_id",
                    type="int",
                    description="ID юридического лица селлера",
                ),
                dg.TableColumn(
                    name="request_uuid",
                    type="string",
                    description="Уникальный идентификатор запроса к API",
                ),
                dg.TableColumn(
                    name="inserted_at",
                    type="timestamp",
                    description="Время вставки записи в данную таблицу",
                ),
                dg.TableColumn(
                    name="advert_id",
                    type="int",
                    description="ID рекламной кампании",
                ),
                dg.TableColumn(
                    name="date",
                    type="timestamp",
                    description="Дата, к которой относится статистика",
                ),
                dg.TableColumn(
                    name="keyword",
                    type="string",
                    description="Ключевое слово, по которому показалась реклама",
                ),
                dg.TableColumn(
                    name="views",
                    type="int",
                    description="Количество показов",
                ),
                dg.TableColumn(
                    name="clicks",
                    type="int",
                    description="Количество кликов",
                ),
                dg.TableColumn(
                    name="ctr",
                    type="float",
                    description="Click-through rate (CTR), отношение кликов к показам",
                ),
                dg.TableColumn(
                    name="cost",
                    type="float",
                    description="Затраты на показы и клики по ключевому слову",
                ),
            ]
        )
    }
