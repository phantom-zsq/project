package com.phantom.other.reflection;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class Main {

	private String name = "main";
	public Integer age;
	private String school;

	public Main(Integer age) {
		this.age = age;
	}

	public Main(String name, Integer age) {

		this.name = name;
		this.age = age;
	}

	public Main(String name, Integer age, String school) {

		this.name = name;
		this.age = age;
		this.school = school;
	}

	public static void main(String[] args) throws Exception {

		// 1. Class类
		// 1.1 静态生成
		Class currentClass = Main.class;
		// 1.2 动态生成
		String className = "com.phantom.other.reflection.Main";
		currentClass = Class.forName(className);

		// 2. 类名
		// 2.1 带包类名
		System.out.println("带包类名：" + currentClass.getName());
		// 2.2 简单类名
		System.out.println("简单类名：" + currentClass.getSimpleName());

		// 3. 类修饰符
		int modifiers = currentClass.getModifiers();
		System.out.println("修饰符值：" + modifiers);
		System.out.println("是否abstract：" + Modifier.isAbstract(modifiers));
		System.out.println("是否final：" + Modifier.isFinal(modifiers));
		System.out.println("是否interface：" + Modifier.isInterface(modifiers));
		System.out.println("是否native：" + Modifier.isNative(modifiers));
		System.out.println("是否private：" + Modifier.isPrivate(modifiers));
		System.out.println("是否protected：" + Modifier.isProtected(modifiers));
		System.out.println("是否public：" + Modifier.isPublic(modifiers));
		System.out.println("是否static：" + Modifier.isStatic(modifiers));
		System.out.println("是否strict：" + Modifier.isStrict(modifiers));
		System.out.println("是否synchronized：" + Modifier.isSynchronized(modifiers));
		System.out.println("是否transient：" + Modifier.isTransient(modifiers));
		System.out.println("是否volatile：" + Modifier.isVolatile(modifiers));

		// 4. 包信息
		Package currentPackage = currentClass.getPackage();
		System.out.println("包名：" + currentPackage.getName());

		// 5. 父类
		Class superClass = currentClass.getSuperclass();

		// 6. 实现的接口
		Class[] interfaces = currentClass.getInterfaces();

		// 7. public构造方法
		// 7.1 全部构造方法
		Constructor[] constructors = currentClass.getConstructors();
		// 7.2 指定构造方法
		Constructor constructor = currentClass.getConstructor(new Class[] { Integer.class });
		// 7.3 构造方法的参数信息
		Class[] constructorParameterTypes = constructor.getParameterTypes();
		// 7.4 实例化一个类
		Main main = (Main) constructor.newInstance(23);

		// 8. public方法
		// 8.1 获取全部方法
		Method[] methods = currentClass.getMethods();
		// 8.2 获取指定方法
		Method method = currentClass.getMethod("print", new Class[] { String.class });
		// 8.3 参数类型
		Class[] methodParameterTypes = method.getParameterTypes();
		// 8.4 返回类型
		Class methodReturnType = method.getReturnType();
		// 8.5 调用方法
		Object publicReturnValue = method.invoke(null, "testMethod");

		// 9.private方法
		Method privateStringMethod = currentClass.getDeclaredMethod("method", null);
		privateStringMethod.setAccessible(true);
		String privateReturnValue = (String) privateStringMethod.invoke(null, null);
		System.out.println("returnValue = " + privateReturnValue);

		// 10. public变量
		// 10.1 全部变量
		Field[] fields = currentClass.getFields();
		// 10.2 指定变量
		Field field = currentClass.getField("age");
		// 10.3 获取变量名
		String fieldName = field.getName();
		// 10.4 获取变量类型
		Object fieldType = field.getType();
		// 10.5 获取或设置（get/set）变量值
		Main tmp1 = new Main(23);
		Object value = field.get(tmp1);
		System.out.println("age字段的值：" + value);
		field.set(tmp1, 24);
		System.out.println("age字段的值：" + tmp1.age);

		// 11. private变量
		Field privateStringField = currentClass.getDeclaredField("school");
		// 关闭反射访问检查
		privateStringField.setAccessible(true);
		Main tmp3 = new Main("name", 23, "xiangtan");
		String school = (String) privateStringField.get(tmp3);
		System.out.println("school = " + school);

		// 12. 注解
		Annotation[] annotations = currentClass.getAnnotations();
	}

	public static void print(String name) {
		System.out.println("print：" + name);
	}

	private static String method() {

		return "private method";
	}
}
