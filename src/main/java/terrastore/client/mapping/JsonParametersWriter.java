/**
 * Copyright 2009 - 2011 Sergio Bossa (sergio.bossa@gmail.com)
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package terrastore.client.mapping;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import org.codehaus.jackson.map.ObjectMapper;

import terrastore.client.Parameters;

/**
 * @author Sergio Bossa
 */
@Provider
@Produces("application/json")
public class JsonParametersWriter implements MessageBodyWriter<Parameters> {

    private final ObjectMapper jsonMapper;

    public JsonParametersWriter() {
        this.jsonMapper = new ObjectMapper();
    }

    public boolean isWriteable(Class type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return Parameters.class.isAssignableFrom(type);
    }

    public void writeTo(Parameters parameters, Class type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap httpHeaders, OutputStream entityStream) throws IOException, WebApplicationException {
        jsonMapper.writeValue(entityStream, parameters);
    }

    public long getSize(Parameters parameters, Class type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }
}
