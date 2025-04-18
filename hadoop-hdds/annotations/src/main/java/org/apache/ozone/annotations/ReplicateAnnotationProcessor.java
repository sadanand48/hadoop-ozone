/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ozone.annotations;

import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic.Kind;

/**
 * Annotation Processor that verifies if the methods that are marked with
 * Replicate annotation have proper method signature which throws
 * TimeoutException.
 */
@SupportedAnnotationTypes(ReplicateAnnotationProcessor.ANNOTATION_NAME)
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class ReplicateAnnotationProcessor extends AbstractProcessor {

  static final String ANNOTATION_NAME =
      "org.apache.hadoop.hdds.scm.metadata.Replicate";
  private static final String REQUIRED_EXCEPTION =
      "org.apache.hadoop.hdds.scm.exceptions.SCMException";
  private TypeMirror requiredException;

  @Override
  public synchronized void init(ProcessingEnvironment processingEnv) {
    super.init(processingEnv);
    requiredException = processingEnv.getElementUtils()
        .getTypeElement(REQUIRED_EXCEPTION).asType();
  }

  @Override
  public boolean process(Set<? extends TypeElement> annotations,
                         RoundEnvironment roundEnv) {
    for (TypeElement annotation : annotations) {
      if (ANNOTATION_NAME.contentEquals(annotation.getQualifiedName())) {
        roundEnv.getElementsAnnotatedWith(annotation)
            .forEach(this::checkMethodSignature);
      }
    }
    return false;
  }

  /**
   * Checks if the method signature is correct.
   *
   * @param element the method element to be checked.
   */
  private void checkMethodSignature(Element element) {
    if (!(element instanceof ExecutableElement)) {
      processingEnv.getMessager().printMessage(Kind.ERROR,
          "Replicate annotation should be on method.");
      return;
    }
    final ExecutableElement executableElement = (ExecutableElement) element;

    if (executableElement.getThrownTypes().stream().noneMatch(
        m -> processingEnv.getTypeUtils().isAssignable(requiredException, m))) {
      processingEnv.getMessager().printMessage(Kind.ERROR,
          "Method with Replicate annotation should declare throwing " +
              REQUIRED_EXCEPTION + " or one of its parents", executableElement);
    }
  }
}
