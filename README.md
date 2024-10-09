# Breweries Pipeline

## Objetivo
O objetivo deste projeto é consumir dados de uma API, transformá-los e armazená-los em um **data lake** utilizando a arquitetura **medallion**, composta por três camadas: **Bronze** (dados brutos), **Silver** (dados curados) e **Gold** (dados analíticos agregados). 

## Descrição Geral do Projeto
Este projeto utiliza a API pública [Open Brewery DB](https://api.openbrewerydb.org/breweries), que disponibiliza informações sobre cervejarias no mundo todo. O foco é criar um pipeline que faça a ingestão dessas informações, organize-as e as armazene de forma estruturada no data lake, de acordo com a arquitetura de três camadas:

1. **Camada Bronze**: Armazenamento dos dados em sua forma original, sem transformações.
2. **Camada Silver**: Limpeza e transformação dos dados, convertendo-os para um formato colunar (como Parquet ou Delta) e particionando-os por estado.
3. **Camada Gold**: Agregação dos dados para análises, como contagem de cervejarias por tipo e localidade.

## Pipeline de Dados

A arquitetura construída consiste em:

![img1](https://i.imgur.com/QILUsJ0.png)

Como o repositório é público, disponibilizei apenas os notebooks Databricks e incluí instruções para a criação dos recursos na nuvem.

## Criação dos Recursos na Nuvem

Os principais recursos criados foram:

Para sustentar a pipeline, foram criados os seguintes recursos na Azure:

1. **Grupo de recursos:** `breweries-case`
2. **Conta de armazenamento habilitada com namespace hierárquico:** `datalakebees`
3. **Criação de container e pastas:**
   - Container: `datalake-bees`
   - Pasta `bronze` para armazenamento de dados brutos.
4. **Serviço do Azure Databricks:** Criação do serviço e configuração de um cluster de processamento DS3_v2 com um único worker.
5. **Registro de aplicativo:** Configurado com permissões de **Colaborador de Dados do Storage Blob**.
6. **Atribuição de permissões:** Configuração de IAM e ACL para o registro de aplicativo no data lake para permitir acesso seguro ao aplicativo.
7. **Serviço do Azure Data Factory:** Criação do serviço para orquestração dos pipelines.

Apenas a pasta **bronze** foi criada no Data Lake, pois as camadas **silver** e **gold** são gerenciadas pelo **Unity Catalog** do Databricks.

## Notebooks Databricks

### 1 - Extração e Ingestão na Camada Bronze

Duas abordagens foram consideradas para a ingestão de dados:

1. Utilizar o Azure Data Factory (ADF) com um conector HTTP para copiar os dados da API e salvá-los diretamente na camada bronze. Isso envolveria um mecanismo de retries em caso de falhas.
2. Implementar um código Python no Databricks para fazer a extração e ingestão dos dados, com tratamento de erros para lidar com possíveis falhas.

Optei pela segunda abordagem devido à necessidade de paginar as requisições na API, o que seria mais eficiente e flexível utilizando o código Python.

O notebook [ImportApiDataBronze](https://github.com/millenagena/brewery-api-pipeline/blob/main/databricks/bronze/ImportApiDataBronze.py) contém o código de extração. Ele inclui as funções:

- **get_data**: Realiza as requisições GET na API, com retries em caso de erro.
- **extract_data**: Faz a paginação e coleta de todos os dados disponíveis.

Após a extração, os dados são armazenados na camada **bronze** do Data Lake.

### 2 - Processamento e Criação da Tabela Silver

O notebook [SilverBreweries](https://github.com/millenagena/brewery-api-pipeline/blob/main/databricks/silver/SilverBreweries.py) faz a curadoria dos dados. As transformações incluem:

- Seleção e conversão das colunas necessárias.
- Filtragem para remover linhas com estados nulos, já que os dados serão particionados por estado.
- Adição de colunas de controle de **data** e **horário** de carregamento.

A decisão de particionar os dados pela coluna **state** foi tomada devido à sua cardinalidade moderada, proporcionando um bom equilíbrio entre granularidade e desempenho.

### 3 - Agregação e Criação da Camada Gold

O notebook [GoldBreweries](https://github.com/millenagena/brewery-api-pipeline/blob/main/databricks/gold/GoldBreweries.py) realiza a agregação dos dados, calculando a quantidade de cervejarias por tipo e estado. Para otimizar o desempenho das consultas futuras, utilizou-se o recurso de **liquid Clustering** para otimização dessa tabela visando futuras consultas com melhor desempenho.

### Unity Catalog

As tabelas silver e gold foram gerenciadas pelo **Unity Catalog**, utilizando dois catálogos específicos para organização e controle:

![img2](https://i.imgur.com/bLqfSLK.png)

Optei pelo Unity Catalog pelos seguintes motivos:

1. **Linhagem de dados**: Fornece rastreabilidade completa desde a origem até as transformações e análises finais.
2. **Segurança centralizada**: Permite controle de acesso robusto e detalhado para as tabelas, essencial em um ambiente colaborativo.
3. **Gerenciamento centralizado**: Simplifica o gerenciamento do data lake e garante consistência nas permissões e políticas de segurança.
4. **Controle de auditoria**: Oferece visibilidade das operações de leitura e escrita no data lake, facilitando auditorias e revisões de conformidade.

A camada **bronze** foi mantida no Data Lake original, pois, conforme as boas práticas do Databricks, o uso de Volumes no Unity Catalog é recomendado apenas para arquivos não tabulares.

## Pipeline - Azure Data Factory (ADF)

Para orquestrar o pipeline de dados, foi configurado um **linked service** no ADF para conectar o Azure Databricks ao Data Factory. Na escolha do cluster, optei pela utilização de um cluster existente no Databricks, ao invés de criar um novo, fiz essa escolha para **reduzir custos** e **otimizar o tempo de execução** durante meus testes. Como os notebooks são responsáveis por toda a extração e transformação de dados, não foi necessária a criação de datasets adicionais no ADF.

### Estrutura do Pipeline

O pipeline foi configurado com as seguintes atividades:

![img3](https://i.imgur.com/gpDjdQk.png)

1. **ImportApiBronzeBreweries**: Executa o notebook de extração de dados da API para a camada **bronze**. Através desta atividade, o pipeline coleta e armazena os dados brutos no Data Lake.

2. **Set status**: Após a execução da atividade anterior, o pipeline armazena o resultado da execução na variável **status**, que é utilizada para verificar o sucesso ou falha da extração. Para isso, foi utilizado o seguinte conteúdo dinâmico:

   ```python
   @activity('ImportApiBronzeBreweries').output.runOutput.status
   ```

3. **If status success**: Esta atividade verifica o valor da variável **status** para garantir que a extração foi bem-sucedida. Utilizando a seguinte expressão dinâmica:

   ```python
   @equals(variables('status'), 'success')
   ```

   - **Caso True**: Se o status for **success**, o pipeline continua executando os notebooks **SilverBreweries** (transformação dos dados para a camada silver) e **GoldBreweries** (agregação para a camada gold).
   - **Caso False**: Se o status for diferente de **success**, o pipeline dispara um erro personalizado com o conteúdo da variável **status**, utilizando o seguinte conteúdo dinâmico:

   ```python
   @concat('Data extraction error: ', variables('status'))
   ```

4. **Pipeline Error Handling**: Caso ocorra uma falha na execução do pipeline, um erro final será gerado. Esse erro pode ser causado por falhas no processamento das tabelas **silver** ou **gold**, ou se a atividade condicional resultar em **False**. A mensagem de erro será capturada e exibida usando o seguinte conteúdo dinâmico:

```python
@activity('If status success')?.Error?.message
```

Essa estrutura permite que o pipeline tome decisões com base no resultado da extração, gerando logs de sucesso ou erros, e interrompendo a execução em caso de falha, o que facilita o monitoramento e resolução de problemas.

### Trigger

Foi configurada uma **trigger baseada em horário**, com execução diária às 7h da manhã, para garantir que os dados sejam atualizados automaticamente todos os dias. 

![img4](https://i.imgur.com/QOmoBh9.png)

Esta configuração atende a um cenário onde a API tem novos dados diariamente, mas para uma estratégia de execução mais precisa seria necessário um entendimento mais aprofundado sobre a frequência de atualização da API.

## Considerações Finais

Para monitoramento e alertas, uma abordagem interessante seria implementar uma tabela de logs onde os erros e as execuções dos notebooks no Databricks fossem armazenados. Esses logs poderiam ser capturados via atividades de **stored procedures** no ADF, garantindo rastreamento de falhas e execuções bem-sucedidas. Além disso, atividades do tipo **Web** no ADF poderiam enviar notificações para canais como Teams, Slack, ou E-mail sempre que o pipeline falhar, permitindo respostas rápidas aos problemas.

Outro ponto importante seria a implementação de um processo de CI/CD (Integração Contínua e Entrega Contínua), o que garantiria a consistência e a versionamento das alterações no pipeline. Isso possibilitaria testes automatizados sempre que novos códigos ou configurações fossem implementados, além de reduzir o tempo para detecção de problemas. Adotar boas práticas de testes, como a validação de schema e simulações de falha no pipeline, também contribuiria para a robustez do projeto e reduziria o risco de erros em produção.
