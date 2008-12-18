

Visão Geral
===========

  RelStorage é uma implementação de storage para o ZODB que provê armazenamento num
banco de dados relacional.  PostgreSQL 8.1 e superior (via psycopg2), MySQL 5.0.x (via
MySQLdb), e Oracle 10g (via cx_Oracle) são atualmente suportados.

  RelStorage substitui o projeto PGStorage.

  Consulte:

     http://wiki.zope.org/ZODB/RelStorage         (wiki)
     http://shane.willowrise.com/                 (blog)
     http://pypi.python.org/pypi/RelStorage       (PyPI entry e downloads)


Características
===============

  * É um substituto para o FileStorage e ZEO.
  * Há um caminho simples para converter FileStorage para RelStorage e reverter novamente. 
Você pode também converter uma instância de RelStorage para um diferente banco de dados relacional.
  * Projetado para sites com grande volume: Múltiplas instâncias do ZODB podem compartilhar o mesmo
banco de dados. Isto é similar para o ZEO, mas RelStorage não requer ZEO.
  * De acordo com alguns testes, RelStorage manipula alta concorrência melhor que
a combinação de ZEO e FileStorage.
  * Enquanto que o FileStorage toma mais tempo para iniciar à medida que o banco de dados cresce, devido a uma
indexação de todos os objetos na memória, RelStorage inicia rapidamente sem levar em consideração o
tamanho do banco de dados.
  * Suporta undo e packing.
  * Livre, open source (ZPL 2.1)


Instalação no Zope
==================

  Você pode instalar RelStorage usando easy_install::

    easy_install RelStorage

  Se você não está usando easy_install (parte do pacote setuptools), você pode
conseguir a última release no PyPI (http://pypi.python.org/pypi/RelStorage), então
instale o pacote relstorage no diretório lib/python de um ou outro
SOFTWARE_HOME ou da INSTANCE_HOME.  Você pode fazer isso como o seguinte
comando::

    python2.4 setup.py install --install-lib=${INSTANCE_HOME}/lib/python

  Antes de você poder usar o RelStorage, ZODB precisa ter o patch de invalidação de checagem
aplicado. Obtenha-o no Subversion (http://svn.zope.org/relstorage/trunk/). 
Há duas versões do patch: uma para ZODB 3.7.1 (que é parte do Zope
2.10.5) e outra para ZODB 3.8.0 (que é parte do Zope 2.11).  O patch não tem
efeito no ZODB exceto quando se está usando o RelStorage.  Esperamos, uma futura release de
ZODB que irá incluir esta característica.

  Você precisa de um adaptador de banco de dados para o Python que corresponda com seu banco de dados. 
Instale psycopg2, MySQLdb 1.2.2+, ou cx_Oracle 4.3+.  Note que o Debian Etch
disponibiliza MySQLdb 1.2.1, mas esta versão tem um bug na manipulação de BLOB que se manifesta
somente com certas configurações de caracteres.  MySQLdb 1.2.2 corrige o
bug.  Também, MySQL 5.1.23 tem bugs maiores que levam a perda de dados quando é feito o packing,
assim MySQL 5.1 não é recomendado neste momento.

  Finalmente, modifique o etc/zope.conf de sua instância de Zope.  Remova o ponto de montagem
principal e adicione um dos seguintes blocos.  Para PostgreSQL::

    %import relstorage
    <zodb_db main>
      mount-point /
      <relstorage>
        <postgresql>
          # O dsn é opcional, assim como são cada parametro no dsn.
          dsn dbname='zodb' user='username' host='localhost' password='pass'
        </postgresql>
      </relstorage>
    </zodb_db>

  Para MySQL::

    %import relstorage
    <zodb_db main>
      mount-point /
      <relstorage>
        <mysql>
          # A maioria das opções providas pelo MySQLdb estão disponíveis.
          # Consulte component.xml.
          db zodb
        </mysql>
      </relstorage>
    </zodb_db>

  Para Oracle (10g XE neste exemplo)::

    %import relstorage
    <zodb_db main>
      mount-point /
      <relstorage>
        <oracle>
          user username
          password pass
          dsn XE
        </oracle>
     </relstorage>
    </zodb_db>

Migrando de FileStorage
=======================

  Você pode converter uma instância de FileStorage para RelStorage e reverter usando o utilitário
chamado ZODBConvert.  Consulte http://wiki.zope.org/ZODB/ZODBConvert.


Migrando de PGStorage
=====================

  O seguinte script migra sua base de dados de PGStorage para RelStorage 1.0
beta:

    migrate.sql_

    .. _migrate.sql:
http://svn.zope.org/*checkout*/relstorage/trunk/notes/migrate.sql

  Após fazer isto, você ainda precisa migrar da versão 1.0 beta para a última
release.


Migrando para uma nova versão do RelStorage
===========================================

  Às vezes o RelStorage necessita de uma modificação de schema junto com uma atualização
do software.  Esperamos, que isto não seja frequentemente necessário.

  Para migrar da versão 1.0 beta para a versão 1.0c1, consulte:

    migrate-1.0-beta.txt_

    .. _migrate-1.0-beta.txt:
http://svn.zope.org/*checkout*/relstorage/trunk/notes/migrate-1.0-beta.txt

  Para migrar da versão 1.0.1 para a versão 1.1b1, consulte:

    migrate-1.0.1.txt_

    .. _migrate-1.0.1.txt:
http://svn.zope.org/*checkout*/relstorage/branches/1.1/notes/migrate-1.0.1.txt


Atributos Opcionais
===================

  poll-interval

    Esta opção é proveitosa se você necessita reduzir o tráfego do banco de dados.  Se configurada,
o RelStorage irá checar mudanças no banco de dados com menos frequência.  Um ajuste de 1 a 5
segundos deverá ser suficiente para a maioria dos sistemas.  Fração de segundos são permitidos.

    Enquanto este ajuste poderia não afetar a integridade do banco de dados, ele aumenta a
probabilidade de transações baseadas em dados antigos, levando a conflitos.  Dessa forma um
ajuste diferente de zero pode danificar a perfomance de servidores com alto volume de escrita.

    Para habilitar este atributo, adicione uma linha similar a "poll-interval 2" dentro de
uma secão <relstorage> do zope.conf.

  pack-gc

    Se pack-gc for falsa, operações de pack não efetuarão a coleta de lixo. 
A coleta de lixo é habilitada por padrão.

    Se o coletor de lixo estiver desabilitado, operações de pack manterão uma de suas menores
revisões de cada objeto.  Com a coleta de lixo desabilitada, o código de pack não
necessita seguir referências de objetos, fazendo packing de modo muito mais rápido. 
No entanto, alguns dos benefícios podem ser perdidos devido a sempre aumentar o número de 
objetos não usados.

    Desabilitando a coleta de lixo é também cortada a segurança que referências inter-database 
nunca quebrem.

    Para desabilitar a coleta de lixo, adicione a linha "pack-gc no" dentro de uma
seção <relstorage> do zope.conf.


Desenvolvimento
===============

  Você pode fazer o checkout do Subversion usando o seguinte comando::

    svn co svn://svn.zope.org/repos/main/relstorage/trunk RelStorage

  Você pode também ver o código:

    http://svn.zope.org/relstorage/trunk/

  O melhor lugar para discutir sobre o desenvolvimento do RelStorage é a lista zodb-dev.



FAQ
===

  P: Como eu posso ajudar?

    R: A melhor caminho de ajudar é testar e prover informações de um banco de dados
específico.  Fazer perguntas sobre RelStorage na lista zodb-dev.

  P: Posso efetuar consultas SQL nos dados no banco de dados?

    R: Não.  Semelhante ao FileStorage e DirectoryStorage, RelStorage armazena os dados
como conserva, tornando difícil para qualquer coisa com exceção do ZODB interpretar os dados.  Um
projeto anterior chamado Ape tentou armazenar dados em um verdadeiro caminho relacional,
mas aconteceu que o Ape trabalhou muito mais contra os princípios do ZODB e
conseqüentemente não pode ser confiável o bastante para uso em produção.  RelStorage, em
outra mão, é muito mais perto para um usual ZODB storage, e é então
muito seguro para uso em produção.

  P: De que forma é comparado o desempenho do RelStorage com FileStorage?

    R: De acordo com benchmarks, RelStorage com PostgreSQL é frequentemente mais rápido que
o FileStorage, especialmente sobre alta concorrência.

  P: Porque eu deveria escolher o RelStorage?

    R: Porque o RelStorage é uma pequena camada que constrói corretamente em bancos de dados
de classe mundial.  Estes bancos de dados tem comprovado confiabilidade e escalabilidade, junto com
numerosas opções de suporte.

  P: O RelStorage pode substituir o ZRS (Zope Replication Services)?

    R: Em teoria, sim.  Com o RelStorage, você pode usar as características de replicação
nativa para seu banco de dados.  No entanto, esta capacidade não foi testada ainda.


Tradução para o Português do Brasil
===================================

Rogerio Ferreira
Site: http://rogerioferreira.objectis.net
Email: rogerio@4linux.com.br