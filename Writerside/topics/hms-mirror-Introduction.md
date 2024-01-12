# Introduction

"hms-mirror" is a utility used to bridge the gap between two clusters and migrate `hive` _metadata_.  HMS-Mirror is distributed under the [APLv2](license) license.

The application will migrate hive metastore data (metadata) between two clusters.  With [SQL](hms-mirror-sql.md) and [EXPORT_IMPORT](hms-mirror-export-import.md) data strategies, we can move data between the two clusters.  While this process functions on smaller datasets, it isn't too efficient for larger datasets.

For the default strategy [SCHEMA_ONLY](hms-mirror-schema-only.md), we can migrate the schemas and sync metastore databases, but the DATA movement is NOT a function of this application.  The application does provide a workbook for each database with SOURCE and TARGET locations.

The output reports are written in [Markdown](https://www.markdownguide.org/).  If you have a client Markdown Renderer like [Marked2](https://marked2app.com/) for the Mac or [Typora](https://typora.io/) which is cross-platform, you'll find a LOT of details in the output reports about what happened.  If you can't install a render, try some web versions [Stackedit.io](https://stackedit.io/app#).  Copy/paste the contents to the report *md* files.
