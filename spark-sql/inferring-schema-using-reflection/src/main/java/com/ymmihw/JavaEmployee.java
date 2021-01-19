package com.ymmihw;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class JavaEmployee implements Serializable {

  private static final long serialVersionUID = 1L;
  private int id;
  private String name;
  private int age;
}
