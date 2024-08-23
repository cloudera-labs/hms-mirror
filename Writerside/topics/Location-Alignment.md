# Location Alignment

In the Hive Metastore, Database definitions, Table Schemas, and Partition details include the location of their
datasets. These locations contain a full URI to the dataset. Migrating from one cluster to another requires us to make
adjustments to these locations.

![datamovement_strategy.png](datamovement_strategy.png)

The most simple translation will change the namespace of the URI so everything is RELATIVE. This helps reduce the
impact of other tools that might be using these datasets outside the Hive Metastore definitions.

Using the `RELATIVE` 'Location Translation Strategy' is suggested for side-car cluster migrations where you
want to keep everything the same as much as possible.

When you are reorganizing, consolidating, or changing storage environments then the `ALIGNED` 'Location Translation 
Strategy' will aid in that process.  We suggest building out [Warehouse Plans](Warehouse-Plans.md) for each database
for maximum control of that movement.

Attributes of location transformations:

* Global Warehouse Directories
* Environment Warehouse Directories

> These are pulling automatically from the Hive Environment when available.

* Target Namespace

> This is defined in the configuration through the `transfer.targetNamespace` configuration attribute or
> the `target-namespace` configuration setting. This is used for migrations between two clusters and for
> STORAGE_MIGRATION's within the cluster.

* Warehouse Plans

> Defined for each 'database'. And when defined we should assume that we expect locations of the database, tables, and
> partitions will be ALIGNED with that location.

## Order of Evaluation

Order of Evaluation means that we will evaluate the attribute in the describe order and once a valid mapping is found,
we will stop the evaluation. Evaluation order depends on the translation type as well.

<tabs>
<tab id="aligned-tab" title="ALIGNED">
<ul>
<li>Target Namespace</li>
<li>Warehouse Plans (when defined)</li>
<li>Global Warehouse Directories</li>
<li>Environment Warehouse Directories (under certain conditions)</li>
</ul>
</tab>
<tab id="relative-tab" title="RELATIVE">
<ul>
<li>Target Namespace</li>
</ul>
</tab>
</tabs>

## Translation Types

Translation types are used to determine how the location should be transformed. The following are the translation types
are `ALIGNED` and `RELATIVE`.

**Legend**

| Icon                                                                           | Description      |
|--------------------------------------------------------------------------------|------------------|
| <img src="checkmarkRound.png" width="30" height="30"/>                         | Valid            |
| <img src="closeRound.png" width="30" height="30"/>                             | Invalid          |
| <img src="ignored.png" width="30" height="30"/>                                | Ignored          |
| <img src="sql-icon.png" width="20" height="20"/>                               | SQL              |
| <img src="files-o-copy.png" width="20" height="20"/>                           | Distcp           |
| <img src="person.png" width="20" height="20"/>                                 | Manual           |
| <img src="typcn-arrow-sync.png" width="20" height="20"/>                       | Automatic        |
| <img src="optional.jpg" width="20" height="20"/>                               | Optional         |
| <img src="linecons-database.png" width="20" height="20"/>                      | Metastore Direct |
| <img src="typcn-world.png" width="20" height="20"/>                            | Global Warehouse |
| <img src="linea--basic-elaboration-document-next.png" width="20" height="20"/> | Warehouse Plan   |

**Translation Scenarios**

<table style="both">
<tr>
<td></td>
<td colspan="2">Translation Type</td>
<td>Data<br/>Movement</td>
<td>Required</td>
<td>Notes</td>
</tr>
<tr>
<td>DUMP</td>
<td><img src="checkmarkRound.png" width="30" height="30"/></td>
<td><p>RELATIVE</p></td>
<td></td>
<td/>
<td/>
</tr>
<tr>
<td></td>
<td><img src="closeRound.png" width="30" height="30"/></td>
<td><p>ALIGNED </p></td>
<td></td>
<td/>
<td/>
</tr>
<tr>
<td>SCHEMA_ONLY</td>
<td><img src="checkmarkRound.png" width="30" height="30"/></td>
<td><p>RELATIVE </p></td>
<td><p><img src="files-o-copy.png" width="20" height="20"/> <img src="person.png" width="20" height="20"/></p></td>
<td/>
<td><img src="typcn-warning.png" width="30" height="30"/> Using distcp <img src="files-o-copy.png" width="20" 
height="20"/> here 
when either 
table or partition 
locations aren't standard, will result in data loss because we're not inspecting all the locations through the Metastore Direct connection.  It's recommended to use `ALIGNED` with `distcp` to build an accurate `distcp` plan.</td>
</tr>
<tr>
<td></td>
<td><img src="checkmarkRound.png" width="30" height="30"/></td>
<td><p>ALIGNED </p></td>
<td><img src="files-o-copy.png" width="20" height="20"/></td>
<td><p><img src="linecons-database.png" width="20" height="20"/> when Warehouse Plan(s) used<br/>
<img src="typcn-world.png" width="20" height="20"/><br/>
<img src="linea--basic-elaboration-document-next.png" width="20" height="20"/>(Optional)</p></td>
<td/>
</tr>
<tr>
<td>SQL</td>
<td><img src="closeRound.png" width="30" height="30"/></td>
<td><p>RELATIVE </p></td>
<td/>
<td/>
<td/>
</tr>
<tr>
<td></td>
<td><img src="checkmarkRound.png" width="30" height="30"/></td>
<td><p>ALIGNED </p></td>
<td><p><img src="sql-icon.png" width="20" height="20"/> <img src="files-o-copy.png" width="20" height="20"/></p></td>
<td><p>
<img src="typcn-world.png" width="20" height="20"/><br/>
<img src="linea--basic-elaboration-document-next.png" width="20" height="20"/>(Optional)
</p>
</td>
<td/>
</tr>
<tr>
<td>EXPORT_IMPORT</td>
<td><img src="checkmarkRound.png" width="30" height="30"/></td>
<td><p>RELATIVE </p></td>
<td/>
<td/>
<td/>
</tr>
<tr>
<td></td>
<td><img src="checkmarkRound.png" width="30" height="30"/></td>
<td><p>ALIGNED </p></td>
<td/>
<td><p>
<img src="typcn-world.png" width="20" height="20"/><br/>
<img src="linea--basic-elaboration-document-next.png" width="20" height="20"/>(Optional)
</p>
</td>
<td></td>
</tr>
<tr>
<td>HYBRID</td>
<td><img src="closeRound.png" width="30" height="30"/></td>
<td><p>RELATIVE </p></td>
<td/>
<td/>
<td/>
</tr>
<tr>
<td></td>
<td><img src="checkmarkRound.png" width="30" height="30"/></td>
<td><p>ALIGNED </p></td>
<td><img src="sql-icon.png" width="20" height="20"/></td>
<td><p>
<img src="typcn-world.png" width="20" height="20"/><br/>
<img src="linea--basic-elaboration-document-next.png" width="20" height="20"/>(Optional)
</p>
</td>
<td/>
</tr>
<tr>
<td>STORAGE_MIGRATION</td>
<td><img src="closeRound.png" width="30" height="30"/></td>
<td><p>RELATIVE </p></td>
<td/>
<td/>
<td/>
</tr>
<tr>
<td></td>
<td><img src="checkmarkRound.png" width="30" height="30"/></td>
<td><p>ALIGNED </p></td>
<td><p><img src="sql-icon.png" width="20" height="20"/>  <img src="files-o-copy.png" width="20" height="20"/></p></td>
<td><p><img src="linecons-database.png" width="20" height="20"/> when Warehouse Plan(s) used<br/>
<img src="linea--basic-elaboration-document-next.png" width="20" height="20"/>(Optional)</p></td>
<td/>
<td/>
</tr>
<tr>
<td>LINKED</td>
<td><img src="ignored.png" width="30" height="30"/></td>
<td><p>RELATIVE </p></td>
<td/>
<td/>
<td/>
</tr>
<tr>
<td></td>
<td><img src="ignored.png" width="30" height="30"/></td>
<td><p>ALIGNED </p></td>
<td/>
<td/>
<td/>
</tr>
<tr>
<td>COMMON</td>
<td><img src="ignored.png" width="30" height="30"/></td>
<td><p>RELATIVE </p></td>
<td/>
<td/>
<td/>
</tr>
<tr>
<td></td>
<td><img src="ignored.png" width="30" height="30"/></td>
<td><p>ALIGNED </p></td>
<td/>
<td/>
<td/>
</tr>
</table>
