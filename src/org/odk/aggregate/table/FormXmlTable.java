/*
 * Copyright (C) 2009 Google Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.odk.aggregate.table;

import com.google.appengine.api.datastore.KeyFactory;

import org.odk.aggregate.PMFactory;
import org.odk.aggregate.constants.BasicConsts;
import org.odk.aggregate.constants.HtmlConsts;
import org.odk.aggregate.constants.HtmlUtil;
import org.odk.aggregate.constants.ServletConsts;
import org.odk.aggregate.constants.TableConsts;
import org.odk.aggregate.form.Form;
import org.odk.aggregate.servlet.FormXmlServlet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

/**
 * Generates an xml description of forms for the servlets
 * 
 * @author wbrunette@gmail.com
 *
 */
public class FormXmlTable {

  private String baseUrl;

  public FormXmlTable(String baseUrl) {
    this.baseUrl = baseUrl + BasicConsts.FORWARDSLASH +FormXmlServlet.ADDR;
  }

  private String generateFormXmlEntry(String odkFormKey, String formName) {
    Map<String, String> properties = new HashMap<String, String>();
    properties.put(ServletConsts.ODK_FORM_KEY, odkFormKey);
    String urlLink = HtmlUtil.createLinkWithProperties(baseUrl, properties);
    return HtmlConsts.BEGIN_OPEN_TAG + TableConsts.FORM_TAG + BasicConsts.SPACE
        + HtmlUtil.createAttribute(TableConsts.URL_ATTR, urlLink) 
        + HtmlConsts.END_TAG + formName + TableConsts.END_FORM_TAG;
  }

  public String generateXmlListOfForms() {
    PersistenceManager pm = PMFactory.get().getPersistenceManager();
    String xml = TableConsts.BEGIN_FORMS_TAG + BasicConsts.NEW_LINE;
    try {
      Query formQuery = pm.newQuery(Form.class);
      @SuppressWarnings("unchecked")
      List<Form> forms = (List<Form>) formQuery.execute();

      // build HTML table of form information
      for (Form form : forms) {
        xml +=
            generateFormXmlEntry(KeyFactory.keyToString(form.getKey()), form.getViewableName())
                + BasicConsts.NEW_LINE;
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      pm.close();
    }

    return xml + TableConsts.END_FORMS_TAG;
  }

}
