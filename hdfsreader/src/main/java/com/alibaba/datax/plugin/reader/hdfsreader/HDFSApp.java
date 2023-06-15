package com.alibaba.datax.plugin.reader.hdfsreader;
//import org.apache.hadoop.conf.Configuration;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.reader.*;
import com.alibaba.datax.plugin.unstructuredstorage.reader.Constant;
import com.alibaba.datax.plugin.unstructuredstorage.reader.Key;
import com.alibaba.fastjson2.JSON;
import com.csvreader.CsvReader;
import io.airlift.compress.snappy.SnappyCodec;
import io.airlift.compress.snappy.SnappyFramedInputStream;
import org.anarres.lzo.LzoDecompressor1x_safe;
import org.anarres.lzo.LzoInputStream;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFileRecordReader;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class HDFSApp {
    private static org.apache.hadoop.conf.Configuration hadoopConf = null;
private enum Type {
    STRING, LONG, BOOLEAN, DOUBLE, DATE, ;
}
    public static void testListFiles() throws URISyntaxException, IOException {
        // 获取FileSystem实例
        String fileUri = "hdfs://11.160.74.200:9000/hivewarehouse/d2c_test_6";
        String splitDelimiter = String.valueOf('\t');
        Map<String, Object> map = new HashMap<>();
        map.put("defaultFS", "hdfs://11.160.74.200:9000/");
        map.put("path", "hdfs://11.160.74.200:9000/");
        map.put("fileType", "TEXT");
        map.put("fieldDelimiter", splitDelimiter);

        Configuration conf = Configuration.from(map);
        String specifiedFileType = conf.getNecessaryValue(com.alibaba.datax.plugin.reader.hdfsreader.Key.FILETYPE,HdfsReaderErrorCode.REQUIRED_VALUE);
        DFSUtil dfsUtil = new DFSUtil(conf);
        List<String> path = new ArrayList<>();
        path.add(fileUri);
        if(specifiedFileType.equalsIgnoreCase(com.alibaba.datax.plugin.reader.hdfsreader.Constant.TEXT)){
            HashSet<String> hash =  dfsUtil.getAllFiles(path, com.alibaba.datax.plugin.reader.hdfsreader.Constant.TEXT);
            List<String> sourceFiles = new ArrayList<>(hash);
            for(String sourceFile : sourceFiles){
                InputStream inputStream = dfsUtil.getInputStream(sourceFile);
                readFromStream(inputStream, sourceFile, conf);
            }
        }else if(specifiedFileType.equalsIgnoreCase(com.alibaba.datax.plugin.reader.hdfsreader.Constant.ORC)){
            HashSet<String> hash =  dfsUtil.getAllFiles(path, com.alibaba.datax.plugin.reader.hdfsreader.Constant.ORC);
            List<String> sourceFiles = new ArrayList<>(hash);
            for(String sourceFile : sourceFiles){
                orcFileStartRead(sourceFile, conf);
            }
        }

    }
    public static void readFromStream(InputStream inputStream, String context,
                                      Configuration readerSliceConfig) {
        String compress = readerSliceConfig.getString(Key.COMPRESS, null);
        if (StringUtils.isBlank(compress)) {
            compress = null;
        }
        String encoding = readerSliceConfig.getString(Key.ENCODING,
                Constant.DEFAULT_ENCODING);
        // handle blank encoding
        if (StringUtils.isBlank(encoding)) {
            encoding = Constant.DEFAULT_ENCODING;
//            LOG.warn(String.format("您配置的encoding为[%s], 使用默认值[%s]", encoding,
//                    Constant.DEFAULT_ENCODING));
        }

        List<Configuration> column = readerSliceConfig
                .getListConfiguration(Key.COLUMN);
        // handle ["*"] -> [], null
        if (null != column && 1 == column.size()
                && "\"*\"".equals(column.get(0).toString())) {
            readerSliceConfig.set(Key.COLUMN, null);
            column = null;
        }

        BufferedReader reader = null;
        int bufferSize = readerSliceConfig.getInt(Key.BUFFER_SIZE,
                Constant.DEFAULT_BUFFER_SIZE);

        // compress logic
        try {
            if (null == compress) {
                reader = new BufferedReader(new InputStreamReader(inputStream,
                        encoding), bufferSize);
            } else {
                // TODO compress
                if ("lzo_deflate".equalsIgnoreCase(compress)) {
                    LzoInputStream lzoInputStream = new LzoInputStream(
                            inputStream, new LzoDecompressor1x_safe());
                    reader = new BufferedReader(new InputStreamReader(
                            lzoInputStream, encoding));
                } else if ("lzo".equalsIgnoreCase(compress)) {
                    LzoInputStream lzopInputStream = new ExpandLzopInputStream(
                            inputStream);
                    reader = new BufferedReader(new InputStreamReader(
                            lzopInputStream, encoding));
                } else if ("gzip".equalsIgnoreCase(compress)) {
                    CompressorInputStream compressorInputStream = new GzipCompressorInputStream(
                            inputStream);
                    reader = new BufferedReader(new InputStreamReader(
                            compressorInputStream, encoding), bufferSize);
                } else if ("bzip2".equalsIgnoreCase(compress)) {
                    CompressorInputStream compressorInputStream = new BZip2CompressorInputStream(
                            inputStream);
                    reader = new BufferedReader(new InputStreamReader(
                            compressorInputStream, encoding), bufferSize);
                } else if ("hadoop-snappy".equalsIgnoreCase(compress)) {
                    CompressionCodec snappyCodec = new SnappyCodec();
                    InputStream snappyInputStream = snappyCodec.createInputStream(
                            inputStream);
                    reader = new BufferedReader(new InputStreamReader(
                            snappyInputStream, encoding));
                } else if ("framing-snappy".equalsIgnoreCase(compress)) {
                    InputStream snappyInputStream = new SnappyFramedInputStream(
                            inputStream);
                    reader = new BufferedReader(new InputStreamReader(
                            snappyInputStream, encoding));
                }/* else if ("lzma".equalsIgnoreCase(compress)) {
					CompressorInputStream compressorInputStream = new LZMACompressorInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							compressorInputStream, encoding));
				} *//*else if ("pack200".equalsIgnoreCase(compress)) {
					CompressorInputStream compressorInputStream = new Pack200CompressorInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							compressorInputStream, encoding));
				} *//*else if ("xz".equalsIgnoreCase(compress)) {
					CompressorInputStream compressorInputStream = new XZCompressorInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							compressorInputStream, encoding));
				} else if ("ar".equalsIgnoreCase(compress)) {
					ArArchiveInputStream arArchiveInputStream = new ArArchiveInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							arArchiveInputStream, encoding));
				} else if ("arj".equalsIgnoreCase(compress)) {
					ArjArchiveInputStream arjArchiveInputStream = new ArjArchiveInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							arjArchiveInputStream, encoding));
				} else if ("cpio".equalsIgnoreCase(compress)) {
					CpioArchiveInputStream cpioArchiveInputStream = new CpioArchiveInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							cpioArchiveInputStream, encoding));
				} else if ("dump".equalsIgnoreCase(compress)) {
					DumpArchiveInputStream dumpArchiveInputStream = new DumpArchiveInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							dumpArchiveInputStream, encoding));
				} else if ("jar".equalsIgnoreCase(compress)) {
					JarArchiveInputStream jarArchiveInputStream = new JarArchiveInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							jarArchiveInputStream, encoding));
				} else if ("tar".equalsIgnoreCase(compress)) {
					TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							tarArchiveInputStream, encoding));
				}*/
                else if ("zip".equalsIgnoreCase(compress)) {
                    ZipCycleInputStream zipCycleInputStream = new ZipCycleInputStream(
                            inputStream);
                    reader = new BufferedReader(new InputStreamReader(
                            zipCycleInputStream, encoding), bufferSize);
                } else {
                    throw DataXException
                            .asDataXException(
                                    UnstructuredStorageReaderErrorCode.ILLEGAL_VALUE,
                                    String.format("仅支持 gzip, bzip2, zip, lzo, lzo_deflate, hadoop-snappy, framing-snappy" +
                                            "文件压缩格式 , 不支持您配置的文件压缩格式: [%s]", compress));
                }
            }
            doReadFromStream(reader, context,
                    readerSliceConfig);
        } catch (UnsupportedEncodingException uee) {
            throw DataXException
                    .asDataXException(
                            UnstructuredStorageReaderErrorCode.OPEN_FILE_WITH_CHARSET_ERROR,
                            String.format("不支持的编码格式 : [%s]", encoding), uee);
        } catch (NullPointerException e) {
            throw DataXException.asDataXException(
                    UnstructuredStorageReaderErrorCode.RUNTIME_EXCEPTION,
                    "运行时错误, 请联系我们", e);
        }/* catch (ArchiveException e) {
			throw DataXException.asDataXException(
					UnstructuredStorageReaderErrorCode.READ_FILE_IO_ERROR,
					String.format("压缩文件流读取错误 : [%s]", context), e);
		} */catch (IOException e) {
            throw DataXException.asDataXException(
                    UnstructuredStorageReaderErrorCode.READ_FILE_IO_ERROR,
                    String.format("流读取错误 : [%s]", context), e);
        } finally {
            org.apache.commons.io.IOUtils.closeQuietly(reader);
        }

    }
    public static List<List<Object>> doReadFromStream(BufferedReader reader, String context,
                                        Configuration readerSliceConfig) {
        List<List<Object>> value = new ArrayList<>();
        String encoding = readerSliceConfig.getString(Key.ENCODING,
                Constant.DEFAULT_ENCODING);
        Character fieldDelimiter = null;
        String delimiterInStr = readerSliceConfig
                .getString(Key.FIELD_DELIMITER);

        if (null != delimiterInStr && 1 != delimiterInStr.length()) {
            throw DataXException.asDataXException(
                    UnstructuredStorageReaderErrorCode.ILLEGAL_VALUE,
                    String.format("仅仅支持单字符切分, 您配置的切分为 : [%s]", delimiterInStr));
        }
        if (null == delimiterInStr) {
//            LOG.warn(String.format("您没有配置列分隔符, 使用默认值[%s]",
//                    Constant.DEFAULT_FIELD_DELIMITER));
            System.out.println(String.format("您没有配置列分隔符, 使用默认值[%s]",
                    Constant.DEFAULT_FIELD_DELIMITER));
        }

        // warn: default value ',', fieldDelimiter could be \n(lineDelimiter)
        // for no fieldDelimiter
        fieldDelimiter = readerSliceConfig.getChar(Key.FIELD_DELIMITER,
                Constant.DEFAULT_FIELD_DELIMITER);
        Boolean skipHeader = readerSliceConfig.getBool(Key.SKIP_HEADER,
                Constant.DEFAULT_SKIP_HEADER);
        // warn: no default value '\N'
        String nullFormat = readerSliceConfig.getString(Key.NULL_FORMAT);

        // warn: Configuration -> List<ColumnEntry> for performance
        // List<Configuration> column = readerSliceConfig
        // .getListConfiguration(Key.COLUMN);
        List<ColumnEntry> column = UnstructuredStorageReaderUtil
                .getListColumnEntry(readerSliceConfig, Key.COLUMN);
        CsvReader csvReader  = null;

        // every line logic
        try {
            // TODO lineDelimiterc

            if (skipHeader) {
                String fetchLine = reader.readLine();
//                LOG.info(String.format("Header line %s has been skiped.",
//                        fetchLine));
                System.out.println(String.format("Header line %s has been skiped.",
                        fetchLine));
            }
            csvReader = new CsvReader(reader);
            csvReader.setDelimiter(fieldDelimiter);

//            setCsvReaderConfig(csvReader);

            String[] parseRows;
            while ((parseRows = UnstructuredStorageReaderUtil
                    .splitBufferedReader(csvReader)) != null) {
                value.add(new ArrayList<>(transportOneRecord(column, parseRows, nullFormat)));
            }
        } catch (UnsupportedEncodingException uee) {
            throw DataXException
                    .asDataXException(
                            UnstructuredStorageReaderErrorCode.OPEN_FILE_WITH_CHARSET_ERROR,
                            String.format("不支持的编码格式 : [%s]", encoding), uee);
        } catch (FileNotFoundException fnfe) {
            throw DataXException.asDataXException(
                    UnstructuredStorageReaderErrorCode.FILE_NOT_EXISTS,
                    String.format("无法找到文件 : [%s]", context), fnfe);
        } catch (IOException ioe) {
            throw DataXException.asDataXException(
                    UnstructuredStorageReaderErrorCode.READ_FILE_IO_ERROR,
                    String.format("读取文件错误 : [%s]", context), ioe);
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    UnstructuredStorageReaderErrorCode.RUNTIME_EXCEPTION,
                    String.format("运行时异常 : %s", e.getMessage()), e);
        } finally {
            csvReader.close();
            org.apache.commons.io.IOUtils.closeQuietly(reader);
        }
        System.out.println(value.toString());
//        for(List list : value)
//            for(Object v : list) System.out.println(v);
        return value;
    }

    /*
    *
    *
    * */
    public static List<List<Object>> orcFileStartRead(String sourceOrcFilePath, Configuration readerSliceConfig) {
//        LOG.info(String.format("Start Read orcfile [%s].", sourceOrcFilePath));
        System.out.println(String.format("Start Read orcfile [%s].", sourceOrcFilePath));
        List<List<Object>> res = new ArrayList<>();
        List<ColumnEntry> column = UnstructuredStorageReaderUtil
                .getListColumnEntry(readerSliceConfig, com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);
        String nullFormat = readerSliceConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.NULL_FORMAT);
        StringBuilder allColumns = new StringBuilder();
        StringBuilder allColumnTypes = new StringBuilder();
        boolean isReadAllColumns = false;
        int columnIndexMax = -1;
        // 判断是否读取所有列
        if (null == column || column.size() == 0) {
            int allColumnsCount = getAllColumnsCount(sourceOrcFilePath);
            columnIndexMax = allColumnsCount - 1;
            isReadAllColumns = true;
        } else {
            columnIndexMax = getMaxIndex(column);
        }
        for (int i = 0; i <= columnIndexMax; i++) {
            allColumns.append("col");
            allColumnTypes.append("string");
            if (i != columnIndexMax) {
                allColumns.append(",");
                allColumnTypes.append(":");
            }
        }
        if (columnIndexMax >= 0) {
            JobConf conf = new JobConf(hadoopConf);
            Path orcFilePath = new Path(sourceOrcFilePath);
            Properties p = new Properties();
            p.setProperty("columns", allColumns.toString());
            p.setProperty("columns.types", allColumnTypes.toString());
            try {
                OrcSerde serde = new OrcSerde();
                serde.initialize(conf, p);
                StructObjectInspector inspector = (StructObjectInspector) serde.getObjectInspector();
                InputFormat<?, ?> in = new OrcInputFormat();
                FileInputFormat.setInputPaths(conf, orcFilePath.toString());

                //If the network disconnected, will retry 45 times, each time the retry interval for 20 seconds
                //Each file as a split
                //TODO multy threads
                // OrcInputFormat getSplits params numSplits not used, splits size = block numbers
                InputSplit[] splits = in.getSplits(conf, -1);
                for (InputSplit split : splits) {
                    {
                        RecordReader reader = in.getRecordReader(split, conf, Reporter.NULL);
                        Object key = reader.createKey();
                        Object value = reader.createValue();
                        // 获取列信息
                        List<? extends StructField> fields = inspector.getAllStructFieldRefs();

                        List<Object> recordFields;
                        while (reader.next(key, value)) {
                            recordFields = new ArrayList<Object>();

                            for (int i = 0; i <= columnIndexMax; i++) {
                                Object field = inspector.getStructFieldData(value, fields.get(i));
                                recordFields.add(field);
                            }
                            res.add(new ArrayList<>(transportOneRecord(column, recordFields,isReadAllColumns, nullFormat)));
                        }
                        reader.close();
                    }
                }
            } catch (Exception e) {
                String message = String.format("从orcfile文件路径[%s]中读取数据发生异常，请联系系统管理员。"
                        , sourceOrcFilePath);
//                LOG.error(message);
                System.out.println(message);
                throw DataXException.asDataXException(HdfsReaderErrorCode.READ_FILE_ERROR, message);
            }
        } else {
            String message = String.format("请确认您所读取的列配置正确！columnIndexMax 小于0,column:%s", JSON.toJSONString(column));
            throw DataXException.asDataXException(HdfsReaderErrorCode.BAD_CONFIG_VALUE, message);
        }
        return res;
    }
    public static void rcFileStartRead(String sourceRcFilePath, Configuration readerSliceConfig) {
//        LOG.info(String.format("Start Read rcfile [%s].", sourceRcFilePath));
        System.out.println(String.format("Start Read rcfile [%s].", sourceRcFilePath));
        List<ColumnEntry> column = UnstructuredStorageReaderUtil
                .getListColumnEntry(readerSliceConfig, com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);
        // warn: no default value '\N'
        String nullFormat = readerSliceConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.NULL_FORMAT);

        Path rcFilePath = new Path(sourceRcFilePath);
        FileSystem fs = null;
        RCFileRecordReader recordReader = null;
        try {
            fs = FileSystem.get(rcFilePath.toUri(), hadoopConf);
            long fileLen = fs.getFileStatus(rcFilePath).getLen();
            FileSplit split = new FileSplit(rcFilePath, 0, fileLen, (String[]) null);
            recordReader = new RCFileRecordReader(hadoopConf, split);
            LongWritable key = new LongWritable();
            BytesRefArrayWritable value = new BytesRefArrayWritable();
            Text txt = new Text();
            while (recordReader.next(key, value)) {
                String[] sourceLine = new String[value.size()];
                txt.clear();
                for (int i = 0; i < value.size(); i++) {
                    BytesRefWritable v = value.get(i);
                    txt.set(v.getData(), v.getStart(), v.getLength());
                    sourceLine[i] = txt.toString();
                }
                transportOneRecord(column, sourceLine, nullFormat);
            }

        } catch (IOException e) {
            String message = String.format("读取文件[%s]时出错", sourceRcFilePath);
//            LOG.error(message);
            System.out.println(message);
            throw DataXException.asDataXException(HdfsReaderErrorCode.READ_RCFILE_ERROR, message, e);
        } finally {
            try {
                if (recordReader != null) {
                    recordReader.close();
//                    LOG.info("Finally, Close RCFileRecordReader.");
                    System.out.println("Finally, Close RCFileRecordReader.");
                }
            } catch (IOException e) {
//                LOG.warn(String.format("finally: 关闭RCFileRecordReader失败, %s", e.getMessage()));
                System.out.println(String.format("finally: 关闭RCFileRecordReader失败, %s", e.getMessage()));
            }
        }

    }
    private static List<Object>  transportOneRecord(List<ColumnEntry> columnConfigs, List<Object> recordFields, boolean isReadAllColumns, String nullFormat) {
        Column columnGenerated;
        List<Object> list = new ArrayList<>();
        try {
            if (isReadAllColumns) {
                // 读取所有列，创建都为String类型的column
                for (Object recordField : recordFields) {
                    String columnValue = null;
                    if (recordField != null) {
                        columnValue = recordField.toString();
                    }
                    columnGenerated = new StringColumn(columnValue);
//                    record.addColumn(columnGenerated);
                }
            } else {
                for (ColumnEntry columnConfig : columnConfigs) {
                    String columnType = columnConfig.getType();
                    Integer columnIndex = columnConfig.getIndex();
                    String columnConst = columnConfig.getValue();

                    String columnValue = null;

                    if (null != columnIndex) {
                        if (null != recordFields.get(columnIndex))
                            columnValue = recordFields.get(columnIndex).toString();
                    } else {
                        columnValue = columnConst;
                    }
                    Type type = Type.valueOf(columnType.toUpperCase());
                    // it's all ok if nullFormat is null
                    if (StringUtils.equals(columnValue, nullFormat)) {
                        columnValue = null;
                    }
                    switch (type) {
                        case STRING:
                            columnGenerated = new StringColumn(columnValue);
                            break;
                        case LONG:
                            try {
                                columnGenerated = new LongColumn(columnValue);
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                        "LONG"));
                            }
                            break;
                        case DOUBLE:
                            try {
                                columnGenerated = new DoubleColumn(columnValue);
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                        "DOUBLE"));
                            }
                            break;
                        case BOOLEAN:
                            try {
                                columnGenerated = new BoolColumn(columnValue);
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                        "BOOLEAN"));
                            }

                            break;
                        case DATE:
                            try {
                                if (columnValue == null) {
                                    columnGenerated = new DateColumn((Date) null);
                                } else {
                                    String formatString = columnConfig.getFormat();
                                    if (StringUtils.isNotBlank(formatString)) {
                                        // 用户自己配置的格式转换
                                        SimpleDateFormat format = new SimpleDateFormat(
                                                formatString);
                                        columnGenerated = new DateColumn(
                                                format.parse(columnValue));
                                    } else {
                                        // 框架尝试转换
                                        columnGenerated = new DateColumn(
                                                new StringColumn(columnValue)
                                                        .asDate());
                                    }
                                }
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                        "DATE"));
                            }
                            break;
                        default:
                            String errorMessage = String.format(
                                    "您配置的列类型暂不支持 : [%s]", columnType);
//                            LOG.error(errorMessage);
                            System.out.println(errorMessage);
                            throw DataXException
                                    .asDataXException(
                                            UnstructuredStorageReaderErrorCode.NOT_SUPPORT_TYPE,
                                            errorMessage);
                    }

//                    record.addColumn(columnGenerated);
                }
            }
        } catch (IllegalArgumentException iae) {
            System.out.println(iae.getMessage());
//                taskPluginCollector
//                        .collectDirtyRecord(record, iae.getMessage());
        } catch (IndexOutOfBoundsException ioe) {
            System.out.println(ioe.getMessage());
//                taskPluginCollector
//                        .collectDirtyRecord(record, ioe.getMessage());
        } catch (Exception e) {
            if (e instanceof DataXException) {
                throw (DataXException) e;
            }
        }
        return list;
    }
    private static int getAllColumnsCount(String filePath) {
        Path path = new Path(filePath);
        try {
            org.apache.hadoop.hive.ql.io.orc.Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(hadoopConf));
            return reader.getTypes().get(0).getSubtypesCount();
        } catch (IOException e) {
            String message = "读取orcfile column列数失败，请联系系统管理员";
            throw DataXException.asDataXException(HdfsReaderErrorCode.READ_FILE_ERROR, message);
        }
    }

    private static int getMaxIndex(List<ColumnEntry> columnConfigs) {
        int maxIndex = -1;
        for (ColumnEntry columnConfig : columnConfigs) {
            Integer columnIndex = columnConfig.getIndex();
            if (columnIndex != null && columnIndex < 0) {
                String message = String.format("您column中配置的index不能小于0，请修改为正确的index,column配置:%s",
                        JSON.toJSONString(columnConfigs));
//                LOG.error(message);
                System.out.println(message);
                throw DataXException.asDataXException(HdfsReaderErrorCode.CONFIG_INVALID_EXCEPTION, message);
            } else if (columnIndex != null && columnIndex > maxIndex) {
                maxIndex = columnIndex;
            }
        }
        return maxIndex;
    }



    public static List<Object> transportOneRecord(List<ColumnEntry> columnConfigs, String[] sourceLine,
                                            String nullFormat) {
        Column columnGenerated = null;
        List<Object> list = new ArrayList<>();
        // 创建都为String类型column的record
        if (null == columnConfigs || columnConfigs.size() == 0) {
            for (String columnValue : sourceLine) {
                // not equalsIgnoreCase, it's all ok if nullFormat is null
                if (columnValue.equals(nullFormat)) {
//                    columnGenerated = new StringColumn(null);
                    list.add("");
                } else {
                    list.add(columnValue);
//                    columnGenerated = new StringColumn(columnValue);
                }

//                record.addColumn(columnGenerated);
            }
        } else {
            try {
                for (ColumnEntry columnConfig : columnConfigs) {
                    String columnType = columnConfig.getType();
                    Integer columnIndex = columnConfig.getIndex();
                    String columnConst = columnConfig.getValue();

                    String columnValue = null;

                    if (null == columnIndex && null == columnConst) {
                        throw DataXException
                                .asDataXException(
                                        UnstructuredStorageReaderErrorCode.NO_INDEX_VALUE,
                                        "由于您配置了type, 则至少需要配置 index 或 value");
                    }

                    if (null != columnIndex && null != columnConst) {
                        throw DataXException
                                .asDataXException(
                                        UnstructuredStorageReaderErrorCode.MIXED_INDEX_VALUE,
                                        "您混合配置了index, value, 每一列同时仅能选择其中一种");
                    }

                    if (null != columnIndex) {
                        if (columnIndex >= sourceLine.length) {
                            String message = String
                                    .format("您尝试读取的列越界,源文件该行有 [%s] 列,您尝试读取第 [%s] 列, 数据详情[%s]",
                                            sourceLine.length, columnIndex + 1,
                                            StringUtils.join(sourceLine, ","));
//                            LOG.warn(message);
                            System.out.println(message);
                            throw new IndexOutOfBoundsException(message);
                        }

                        columnValue = sourceLine[columnIndex];
                    } else {
                        columnValue = columnConst;
                    }
                    Type type = Type.valueOf(columnType.toUpperCase());
                    if (columnValue.equals(nullFormat)) {
                        columnValue = null;
                    }
                    switch (type) {
                        case STRING:
                            columnGenerated = new StringColumn(columnValue);
                            break;
                        case LONG:
                            try {
                                columnGenerated = new LongColumn(columnValue);
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                        "LONG"));
                            }
                            break;
                        case DOUBLE:
                            try {
                                columnGenerated = new DoubleColumn(columnValue);
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                        "DOUBLE"));
                            }
                            break;
                        case BOOLEAN:
                            try {
                                columnGenerated = new BoolColumn(columnValue);
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                        "BOOLEAN"));
                            }

                            break;
                        case DATE:
                            try {
                                if (columnValue == null) {
                                    Date date = null;
                                    columnGenerated = new DateColumn(date);
                                } else {
                                    String formatString = columnConfig.getFormat();
                                    //if (null != formatString) {
                                    if (StringUtils.isNotBlank(formatString)) {
                                        // 用户自己配置的格式转换, 脏数据行为出现变化
                                        DateFormat format = columnConfig
                                                .getDateFormat();
                                        columnGenerated = new DateColumn(
                                                format.parse(columnValue));
                                    } else {
                                        // 框架尝试转换
                                        columnGenerated = new DateColumn(
                                                new StringColumn(columnValue)
                                                        .asDate());
                                    }
                                }
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                        "DATE"));
                            }
                            break;
                        default:
                            String errorMessage = String.format(
                                    "您配置的列类型暂不支持 : [%s]", columnType);
                            System.out.println(errorMessage);
                            throw DataXException
                                    .asDataXException(
                                            UnstructuredStorageReaderErrorCode.NOT_SUPPORT_TYPE,
                                            errorMessage);
                    }
                    list.add(columnGenerated);

                }
            } catch (IllegalArgumentException iae) {
                System.out.println(iae.getMessage());
//                taskPluginCollector
//                        .collectDirtyRecord(record, iae.getMessage());
            } catch (IndexOutOfBoundsException ioe) {
                System.out.println(ioe.getMessage());
//                taskPluginCollector
//                        .collectDirtyRecord(record, ioe.getMessage());
            } catch (Exception e) {
                if (e instanceof DataXException) {
                    throw (DataXException) e;
                }
            }
        }
        return list;
    }
    public static void main(String[] args) throws IOException, URISyntaxException {
        System.out.println("begin -----");
        testListFiles();
    }
}
