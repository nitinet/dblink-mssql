import { Handler, model } from 'dblink-core';
import mssql from 'mssql';
export default class MsSqlServer extends Handler {
  connectionPool;
  constructor(config) {
    super(config);
    this.connectionPool = new mssql.ConnectionPool(config);
  }
  async init() {
    await this.connectionPool.connect();
  }
  async getConnection() {
    await mssql.connect(this.config);
    return new mssql.Request();
  }
  async initTransaction() {}
  async commit() {}
  async rollback() {}
  async close() {}
  async run(query, dataArgs, connection) {
    const conn = connection ?? this.connectionPool.request();
    const data = await conn.query(query);
    const result = new model.ResultSet();
    result.rows = data.recordset;
    return result;
  }
  runStatement(queryStmt, connection) {
    const { query, dataArgs } = this.prepareQuery(queryStmt);
    return this.run(query, dataArgs, connection);
  }
  async stream(query, dataArgs, connection) {
    const conn = connection ?? this.connectionPool.request();
    conn.stream = true;
    conn.query(query);
    return conn.toReadableStream();
  }
  streamStatement(queryStmt, connection) {
    const { query, dataArgs } = this.prepareQuery(queryStmt);
    return this.stream(query, dataArgs, connection);
  }
}
//# sourceMappingURL=index.js.map
