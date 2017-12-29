// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: CustomFilter.proto

package edu.zju.gis.hbase.tool;

public final class CustomFilterProto {
  private CustomFilterProto() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface CustomFilterOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // required bytes wkt = 1;
    /**
     * <code>required bytes wkt = 1;</code>
     */
    boolean hasWkt();
    /**
     * <code>required bytes wkt = 1;</code>
     */
    com.google.protobuf.ByteString getWkt();
  }
  /**
   * Protobuf type {@code CustomFilter}
   */
  public static final class CustomFilter extends
      com.google.protobuf.GeneratedMessage
      implements CustomFilterOrBuilder {
    // Use CustomFilter.newBuilder() to construct.
    private CustomFilter(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private CustomFilter(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final CustomFilter defaultInstance;
    public static CustomFilter getDefaultInstance() {
      return defaultInstance;
    }

    public CustomFilter getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private CustomFilter(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              bitField0_ |= 0x00000001;
              wkt_ = input.readBytes();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return edu.zju.gis.hbase.tool.CustomFilterProto.internal_static_CustomFilter_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return edu.zju.gis.hbase.tool.CustomFilterProto.internal_static_CustomFilter_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter.class, edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter.Builder.class);
    }

    public static com.google.protobuf.Parser<CustomFilter> PARSER =
        new com.google.protobuf.AbstractParser<CustomFilter>() {
      public CustomFilter parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new CustomFilter(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<CustomFilter> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // required bytes wkt = 1;
    public static final int WKT_FIELD_NUMBER = 1;
    private com.google.protobuf.ByteString wkt_;
    /**
     * <code>required bytes wkt = 1;</code>
     */
    public boolean hasWkt() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required bytes wkt = 1;</code>
     */
    public com.google.protobuf.ByteString getWkt() {
      return wkt_;
    }

    private void initFields() {
      wkt_ = com.google.protobuf.ByteString.EMPTY;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      if (!hasWkt()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBytes(1, wkt_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(1, wkt_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code CustomFilter}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilterOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return edu.zju.gis.hbase.tool.CustomFilterProto.internal_static_CustomFilter_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return edu.zju.gis.hbase.tool.CustomFilterProto.internal_static_CustomFilter_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter.class, edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter.Builder.class);
      }

      // Construct using edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        wkt_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return edu.zju.gis.hbase.tool.CustomFilterProto.internal_static_CustomFilter_descriptor;
      }

      public edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter getDefaultInstanceForType() {
        return edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter.getDefaultInstance();
      }

      public edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter build() {
        edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter buildPartial() {
        edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter result = new edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.wkt_ = wkt_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter) {
          return mergeFrom((edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter other) {
        if (other == edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter.getDefaultInstance()) return this;
        if (other.hasWkt()) {
          setWkt(other.getWkt());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        if (!hasWkt()) {
          
          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (edu.zju.gis.hbase.tool.CustomFilterProto.CustomFilter) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // required bytes wkt = 1;
      private com.google.protobuf.ByteString wkt_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>required bytes wkt = 1;</code>
       */
      public boolean hasWkt() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required bytes wkt = 1;</code>
       */
      public com.google.protobuf.ByteString getWkt() {
        return wkt_;
      }
      /**
       * <code>required bytes wkt = 1;</code>
       */
      public Builder setWkt(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        wkt_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required bytes wkt = 1;</code>
       */
      public Builder clearWkt() {
        bitField0_ = (bitField0_ & ~0x00000001);
        wkt_ = getDefaultInstance().getWkt();
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:CustomFilter)
    }

    static {
      defaultInstance = new CustomFilter(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:CustomFilter)
  }

  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_CustomFilter_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_CustomFilter_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\022CustomFilter.proto\"\033\n\014CustomFilter\022\013\n\003" +
      "wkt\030\001 \002(\014B+\n\026edu.zju.gis.hbase.toolB\021Cus" +
      "tomFilterProto"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_CustomFilter_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_CustomFilter_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_CustomFilter_descriptor,
              new java.lang.String[] { "Wkt", });
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }

  // @@protoc_insertion_point(outer_class_scope)
}