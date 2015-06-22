/*
 * Copyright (C) 2013 Christian Autermann
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package com.github.autermann.wps.commons;

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;

import java.util.Collections;
import java.util.Iterator;

import net.opengis.wps.x100.ComplexDataCombinationType;
import net.opengis.wps.x100.ComplexDataCombinationsType;
import net.opengis.wps.x100.ComplexDataDescriptionType;
import net.opengis.wps.x100.ComplexDataType;
import net.opengis.wps.x100.DocumentOutputDefinitionType;
import net.opengis.wps.x100.InputDescriptionType;
import net.opengis.wps.x100.InputReferenceType;
import net.opengis.wps.x100.OutputDefinitionType;
import net.opengis.wps.x100.OutputDescriptionType;
import net.opengis.wps.x100.OutputReferenceType;
import net.opengis.wps.x100.SupportedComplexDataType;

import org.n52.wps.FormatDocument;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;

/**
 * TODO JavaDoc
 *
 * @author Christian Autermann
 */
public class Format {

    private final Optional<String> mimeType;
    private final Optional<String> encoding;
    private final Optional<String> schema;

    public Format(String mimeType) {
        this(mimeType, null, null);
    }

    public Format(String mimeType, String encoding) {
        this(mimeType, encoding, null);
    }

    public Format(String mimeType, String encoding, String schema) {
        this.mimeType = fromNullable(emptyToNull(mimeType));
        this.encoding = fromNullable(emptyToNull(encoding));
        this.schema = fromNullable(emptyToNull(schema));
    }

    public Format() {
        this(null, null, null);
    }

    public Optional<String> getMimeType() {
        return mimeType;
    }

    public Optional<String> getEncoding() {
        return encoding;
    }

    public Optional<String> getSchema() {
        return schema;
    }

    public boolean isEmpty() {
        return !hasMimeType() && !hasEncoding() && !hasSchema();
    }

    public boolean hasSchema() {
        return getSchema().isPresent();
    }

    public boolean hasEncoding() {
        return getEncoding().isPresent();
    }

    public boolean hasMimeType() {
        return getMimeType().isPresent();
    }

    public boolean hasMimeType(String mimeType) {
        return getMimeType().or("").equalsIgnoreCase(nullToEmpty(mimeType));
    }

    public boolean hasEncoding(String encoding) {
        return getEncoding().or("").equalsIgnoreCase(nullToEmpty(encoding));
    }

    public boolean hasSchema(String schema) {
        return getSchema().or("").equalsIgnoreCase(nullToEmpty(schema));
    }

    public boolean hasMimeType(Format other) {
        return hasMimeType(other.getMimeType().orNull());
    }

    public boolean hasEncoding(Format other) {
        return hasEncoding(other.getEncoding().orNull());
    }

    public boolean hasSchema(Format other) {
        return hasSchema(other.getSchema().orNull());
    }

    public boolean matchesMimeType(String mimeType) {
        return !hasMimeType() || hasMimeType(mimeType);
    }

    public boolean matchesEncoding(String encoding) {
        return !hasEncoding() || hasEncoding(encoding);
    }

    public boolean matchesSchema(String schema) {
        return !hasSchema() || hasSchema(schema);
    }

    public boolean matchesMimeType(Format other) {
        return !hasMimeType() || hasMimeType(other);
    }

    public boolean matchesEncoding(Format other) {
        return !hasEncoding() || hasEncoding(other);
    }

    public boolean matchesSchema(Format other) {
        return !hasSchema() || hasSchema(other);
    }

    public Format withEncoding(String encoding) {
        return new Format(getMimeType().orNull(), encoding, getSchema().orNull());
    }

    public Format withBase64Encoding() {
        return withEncoding("Base64");
    }

    public Format withUTF8Encoding() {
        return withEncoding("UTF-8");
    }

    public Format withSchema(String schema) {
        return new Format(getMimeType().orNull(), getEncoding().orNull(), schema);
    }

    public Format withMimeType(String mimeType) {
        return new Format(mimeType, getEncoding().orNull(), getSchema().orNull());
    }

    public Format withoutMimeType() {
        return new Format(null, getEncoding().orNull(), getSchema().orNull());
    }

    public Format withoutEncoding() {
        return new Format(getMimeType().orNull(), null, getSchema().orNull());
    }

    public Format withoutSchema() {
        return new Format(getMimeType().orNull(), getEncoding().orNull(), null);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).omitNullValues()
                .add("mimeType", this.mimeType.orNull())
                .add("encoding", this.encoding.orNull())
                .add("schema", this.schema.orNull()).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.mimeType, this.encoding, this.schema);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Format) {
            final Format that = (Format) obj;
            return Objects.equal(this.mimeType, that.getMimeType()) &&
                   Objects.equal(this.encoding, that.getEncoding()) &&
                   Objects.equal(this.schema, that.getSchema());
        }
        return false;
    }

    public Predicate<Format> matchingEncoding() {
        return new Predicate<Format>() {

            @Override
            public boolean apply(Format input) {
                return hasEncoding(input);
            }
        };
    }

    public Predicate<Format> matchingSchema() {
        return new Predicate<Format>() {

            @Override
            public boolean apply(Format input) {
                return hasSchema(input);
            }
        };
    }

    public Predicate<Format> matchingMimeType() {
        return new Predicate<Format>() {

            @Override
            public boolean apply(Format input) {
                return hasMimeType(input);
            }
        };
    }

    public void encodeTo(InputReferenceType irt) {
        if (hasMimeType()) {
            irt.setMimeType(getMimeType().get());
        }
        if (hasEncoding()) {
            irt.setEncoding(getEncoding().get());
        }
        if (hasSchema()) {
            irt.setSchema(getSchema().get());
        }
    }

    public void encodeTo(DocumentOutputDefinitionType dodt) {
        if (hasMimeType()) {
            dodt.setMimeType(getMimeType().get());
        }
        if (hasEncoding()) {
            dodt.setEncoding(getEncoding().get());
        }
        if (hasSchema()) {
            dodt.setSchema(getSchema().get());
        }
    }

    public void encodeTo(ComplexDataDescriptionType cddt) {
        if (hasMimeType()) {
            cddt.setMimeType(getMimeType().get());
        }
        if (hasEncoding()) {
            cddt.setEncoding(getEncoding().get());
        }
        if (hasSchema()) {
            cddt.setSchema(getSchema().get());
        }
    }

    public void encodeTo(ComplexDataType cdt) {
        if (hasMimeType()) {
            cdt.setMimeType(getMimeType().get());
        }
        if (hasEncoding()) {
            cdt.setEncoding(getEncoding().get());
        }
        if (hasSchema()) {
            cdt.setSchema(getSchema().get());
        }
    }

    public void encodeTo(OutputReferenceType ort) {
        if (hasMimeType()) {
            ort.setMimeType(getMimeType().get());
        }
        if (hasEncoding()) {
            ort.setEncoding(getEncoding().get());
        }
        if (hasSchema()) {
            ort.setSchema(getSchema().get());
        }
    }

    public void encodeTo(FormatDocument.Format f) {
        if (hasMimeType()) {
            f.setMimetype(getMimeType().get());
        }
        if (hasEncoding()) {
            f.setEncoding(getEncoding().get());
        }
        if (hasSchema()) {
            f.setSchema(getSchema().get());
        }
    }

    public static Format of(OutputDefinitionType odt) {
        return new Format(odt.getMimeType(),
                          odt.getEncoding(),
                          odt.getSchema());
    }

    public static Format of(ComplexDataType cdt) {
        return new Format(cdt.getMimeType(),
                          cdt.getEncoding(),
                          cdt.getSchema());
    }

    public static Format of(ComplexDataCombinationType cddct) {
        return of(cddct.getFormat());
    }

    public static Format of(InputReferenceType irt) {
        return new Format(irt.getMimeType(),
                          irt.getEncoding(),
                          irt.getSchema());
    }

    public static Format of(ComplexDataDescriptionType cddt) {
        return new Format(cddt.getMimeType(),
                          cddt.getEncoding(),
                          cddt.getSchema());
    }

    public static Iterable<Format> of(ComplexDataDescriptionType[] list) {
        if (list == null) {
            return Collections.emptyList();
        } else {
            return new ComplexDataDescriptionTypeIterable(list);
        }

    }

    public static Iterable<Format> of(ComplexDataCombinationsType list) {
        if (list == null) {
            return Collections.emptyList();
        } else {
            return of(list.getFormatArray());
        }
    }

    public static Format getDefault(InputDescriptionType idt) {
        if (idt == null) {
            return null;
        } else {
            return getDefault(idt.getComplexData());
        }
    }

    public static Iterable<Format> getSupported(InputDescriptionType idt) {
        if (idt == null) {
            return Collections.emptyList();
        } else {
            return getSupported(idt.getComplexData());
        }
    }

    public static Format getDefault(OutputDescriptionType odt) {
        if (odt == null) {
            return null;
        } else {
            return getDefault(odt.getComplexOutput());
        }
    }

    public static Iterable<Format> getSupported(OutputDescriptionType idt) {
        if (idt == null) {
            return Collections.emptyList();
        } else {
            return getSupported(idt.getComplexOutput());
        }
    }

    public static Iterable<Format> getSupported(SupportedComplexDataType scdt) {
        if (scdt == null) {
            return Collections.emptyList();
        } else {
            return of(scdt.getSupported());
        }
    }

    public static Format getDefault(SupportedComplexDataType scdt) {
        if (scdt == null) {
            return null;
        } else {
            return of(scdt.getDefault());
        }
    }

    private static class ComplexDataDescriptionTypeIterable
            implements Iterable<Format> {
        private final ComplexDataDescriptionType[] supported;

        ComplexDataDescriptionTypeIterable(
                ComplexDataDescriptionType[] supported) {
            this.supported = checkNotNull(supported);
        }

        @Override
        public Iterator<Format> iterator() {
            return new ComplexDataDescriptionTypeIterator(supported);
        }

    }

    private static class ComplexDataDescriptionTypeIterator
            extends UnmodifiableIterator<Format> {
        final Iterator<ComplexDataDescriptionType> iter;

        ComplexDataDescriptionTypeIterator(
                ComplexDataDescriptionType[] supported) {
            this.iter = Iterators.forArray(supported);
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public Format next() {
            return Format.of(iter.next());
        }
    }
}
